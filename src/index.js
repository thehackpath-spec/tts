var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

var index_default = {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/" && request.method === "GET") {
      return new Response("Summer Donations TTS is online.", {
        status: 200,
        headers: {
          "Content-Type": "text/plain; charset=UTF-8"
        }
      });
    }
    const overlayMatch = url.pathname.match(/^\/users\/(\d+)\/overlay$/);
    if (overlayMatch && request.method === "GET") {
      const userId = overlayMatch[1];
      const tts = parseBoolean(
        url.searchParams.get("tts"),
        true
      );
      const minAmount = clampNumber(
        url.searchParams.get("minAmount"),
        0,
        1e9,
        0
      );
      const maxLength = clampNumber(
        url.searchParams.get("maxLength"),
        1,
        300,
        100
      );
      return new Response(
        createOverlayHTML({
          userId,
          tts,
          minAmount,
          maxLength
        }),
        {
          status: 200,
          headers: {
            "Content-Type": "text/html; charset=UTF-8",
            "Cache-Control": "no-store"
          }
        }
      );
    }
    const streamMatch = url.pathname.match(/^\/api\/stream\/(\d+)$/);
    if (streamMatch && request.method === "GET") {
      const userId = streamMatch[1];
      if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
        return new Response("WebSocket required.", {
          status: 426
        });
      }
      const id = env.DONATION_QUEUE.idFromName(userId);
      const queue = env.DONATION_QUEUE.get(id);
      return queue.fetch(
        new Request("https://internal/connect", {
          method: "GET",
          headers: request.headers
        })
      );
    }
    if (url.pathname === "/api/donation" && request.method === "POST") {
      return handleDonation(request, env);
    }
    return new Response("Not found.", {
      status: 404
    });
  }
};

async function handleDonation(request, env) {
  const authorization = request.headers.get("Authorization");
  if (!authorization || !authorization.startsWith("Bearer ")) {
    return json({
      ok: false,
      error: "Unauthorized"
    }, 401);
  }
  const suppliedSecret = authorization.slice(7);
  if (!env.SECRET) {
    return json({
      ok: false,
      error: "Server configuration error"
    }, 500);
  }
  if (!safeEqual(suppliedSecret, env.SECRET)) {
    return json({
      ok: false,
      error: "Unauthorized"
    }, 401);
  }
  let body;
  try {
    body = await request.json();
  } catch (err) {
    return json({
      ok: false,
      error: "Invalid JSON"
    }, 400);
  }
  const recipientUserId = String(
    body.recipientUserId ?? ""
  ).trim();
  const donorName = cleanText(
    body.donorName,
    50
  );
  const amount = Number(
    body.amount
  );
  const message = cleanText(
    body.message,
    300
  );
  if (!/^\d+$/.test(recipientUserId)) {
    return json({
      ok: false,
      error: "Invalid recipientUserId"
    }, 400);
  }
  if (!donorName) {
    return json({
      ok: false,
      error: "Missing donorName"
    }, 400);
  }
  if (!Number.isFinite(amount) || amount < 0 || amount > 1e9) {
    return json({
      ok: false,
      error: "Invalid amount"
    }, 400);
  }
  const donation = {
    id: crypto.randomUUID(),
    recipientUserId,
    donorName,
    amount: Math.floor(amount),
    message,
    createdAt: Date.now()
  };

  const id = env.DONATION_QUEUE.idFromName(
    recipientUserId
  );
  const queue = env.DONATION_QUEUE.get(id);
  const response = await queue.fetch(
    new Request("https://internal/donation", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(donation)
    })
  );
  return new Response(
    await response.text(),
    {
      status: response.status,
      headers: {
        "Content-Type": "application/json"
      }
    }
  );
}
__name(handleDonation, "handleDonation");

var DonationQueue = class {
  static {
    __name(this, "DonationQueue");
  }
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }
  async alarm() {
    const sockets = this.state.getWebSockets();
    if (sockets.length === 0) {
      await this.state.storage.put("connected", false);
      await this.state.storage.put("sending", false);
      await this.state.storage.put("queue", []);
    } else {
      await this.state.storage.put("sending", false);
      await this.sendNext();
    }
  }
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/connect" && request.method === "GET") {
      return this.connect(request);
    }
    if (url.pathname === "/donation" && request.method === "POST") {
      return this.receiveDonation(request);
    }
    if (url.pathname === "/ready" && request.method === "POST") {
      return this.nextDonation();
    }
    return new Response("Not found.", { status: 404 });
  }
  async connect(request) {
    await this.state.storage.deleteAlarm();
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    this.state.acceptWebSocket(server);
    await this.state.storage.put("connected", true);
    server.send(JSON.stringify({ type: "connected" }));
    return new Response(null, {
      status: 101,
      webSocket: client
    });
  }
  async receiveDonation(request) {
    const connected = await this.state.storage.get("connected");
    const sockets = this.state.getWebSockets();
    if (!connected && sockets.length === 0) {
      return json({
        ok: true,
        delivered: false,
        reason: "offline"
      });
    }
    let donation;
    try {
      donation = await request.json();
    } catch (err) {
      return json({ ok: false, error: "Invalid donation" }, 400);
    }
    const queue = await this.state.storage.get("queue") || [];
    queue.push(donation);
    if (queue.length > 100) {
      queue.splice(0, queue.length - 100);
    }
    await this.state.storage.put("queue", queue);
    await this.sendNext();
    return json({ ok: true, delivered: true });
  }
  async nextDonation() {
    await this.state.storage.put("sending", false);
    const lastSentTime = await this.state.storage.get("lastSentTime") || 0;
    const timeSinceLast = Date.now() - lastSentTime;

    if (timeSinceLast < 2e3) {
      const waitTime = 2e3 - timeSinceLast;
      await this.state.storage.setAlarm(Date.now() + waitTime);
      return json({ ok: true, ignored: true });
    }

    await this.sendNext();
    return json({ ok: true });
  }
  async sendNext() {
    const connected = await this.state.storage.get("connected");
    if (!connected) {
      return;
    }
    const sending = await this.state.storage.get("sending");
    if (sending) {
      return;
    }
    const queue = await this.state.storage.get("queue") || [];
    if (queue.length === 0) {
      return;
    }
    const sockets = this.state.getWebSockets();
    if (sockets.length === 0) {
      await this.state.storage.setAlarm(Date.now() + 6e3);
      return;
    }
    const donation = queue.shift();
    await this.state.storage.put("queue", queue);
    await this.state.storage.put("sending", true);
    await this.state.storage.put("lastSentTime", Date.now());

    const payload = JSON.stringify({
      type: "donation",
      donation
    });
    for (const socket of sockets) {
      try {
        socket.send(payload);
      } catch (err) {
      }
    }
  }
  async webSocketClose() {
    const sockets = this.state.getWebSockets();
    if (sockets.length === 0) {
      await this.state.storage.setAlarm(Date.now() + 6e3);
    }
  }
  async webSocketError() {
    await this.webSocketClose();
  }
  async webSocketMessage(ws, message) {
    if (message === "ready") {
      await this.nextDonation();
    }
  }
};

function createOverlayHTML({
  userId,
  tts,
  minAmount,
  maxLength
}) {
  const config = JSON.stringify({
    userId,
    tts,
    minAmount,
    maxLength
  });
  return `<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Summer Donations TTS</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Poppins:ital,wght@0,700;0,800;0,900;1,800&display=swap" rel="stylesheet">

<style>
html, body {
  margin: 0;
  padding: 0;
  width: 100vw;
  height: 100vh;
  background: transparent !important;
  overflow: hidden;
  font-family: 'Poppins', sans-serif;
  display: flex;
  justify-content: center;
  align-items: center;
}

#donation {
  position: fixed;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%) scale(0.6);
  max-width: 90vw;
  width: max-content;
  text-align: center;
  opacity: 0;
  pointer-events: none;
  word-wrap: break-word;
  background: transparent !important;
  transition: all 0.5s cubic-bezier(0.175, 0.885, 0.32, 1.275);
}

#donation.show {
  opacity: 1;
  transform: translate(-50%, -50%) scale(1);
  animation: floatBounce 3s ease-in-out infinite alternate;
}

@keyframes floatBounce {
  0% {
    transform: translate(-50%, -50%) scale(1) translateY(0px);
  }
  100% {
    transform: translate(-50%, -50%) scale(1.03) translateY(-12px);
  }
}

.donation-header {
  font-size: 46px;
  font-weight: 900;
  line-height: 1.25;
  text-transform: uppercase;
  letter-spacing: 1.5px;
}

.donor-name {
  color: #ff3366;
  text-shadow: 0 0 12px rgba(255, 51, 102, 0.7), 2.5px 2.5px 0px #000, -2.5px -2.5px 0px #000, 2.5px -2.5px 0px #000, -2.5px 2.5px 0px #000, 0 5px 15px rgba(0,0,0,0.9);
}

.action-text {
  color: #ffffff;
  text-shadow: 2.5px 2.5px 0px #000, -2.5px -2.5px 0px #000, 2.5px -2.5px 0px #000, -2.5px 2.5px 0px #000, 0 5px 15px rgba(0,0,0,0.9);
}

.amount-text {
  color: #00ffcc;
  text-shadow: 0 0 12px rgba(0, 255, 204, 0.7), 2.5px 2.5px 0px #000, -2.5px -2.5px 0px #000, 2.5px -2.5px 0px #000, -2.5px 2.5px 0px #000, 0 5px 15px rgba(0,0,0,0.9);
}

.donation-message {
  margin-top: 18px;
  font-size: 34px;
  font-weight: 800;
  color: #fffb00;
  text-shadow: 0 0 12px rgba(255, 251, 0, 0.6), 2.5px 2.5px 0px #000, -2.5px -2.5px 0px #000, 2.5px -2.5px 0px #000, -2.5px 2.5px 0px #000, 0 5px 15px rgba(0,0,0,0.9);
}
</style>
</head>

<body>

<div id="donation"></div>

<script>
const CONFIG = ${config};

const donationElement = document.getElementById("donation");

let socket = null;
let reconnectTimer = null;

if ('speechSynthesis' in window) {
  window.speechSynthesis.onvoiceschanged = () => {
    window.speechSynthesis.getVoices();
  };
}

function escapeHTML(value) {
  const element = document.createElement("div");
  element.textContent = value;
  return element.innerHTML;
}

function connect() {
  if (
    socket &&
    (
      socket.readyState === WebSocket.OPEN ||
      socket.readyState === WebSocket.CONNECTING
    )
  ) {
    return;
  }

  const protocol = location.protocol === "https:" ? "wss:" : "ws:";
  const url = protocol + "//" + location.host + "/api/stream/" + encodeURIComponent(CONFIG.userId);

  socket = new WebSocket(url);

  socket.onopen = () => {
    socket.send("ready");
  };

  socket.onmessage = async (event) => {
    let data;

    try {
      data = JSON.parse(event.data);
    } catch {
      return;
    }

    if (data.type !== "donation") {
      return;
    }

    await playDonation(data.donation);
  };

  socket.onclose = () => {
    socket = null;
    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(connect, 1000);
  };

  socket.onerror = () => {
    try {
      socket.close();
    } catch {}
  };
}

function speak(text) {
  if (!('speechSynthesis' in window)) {
    donationElement.innerHTML = '<div style="color: red; font-size: 24px;">TTS ERROR: speechSynthesis NOT supported by OBS!</div>';
    donationElement.classList.add("show");
    return Promise.resolve();
  }

  return new Promise((resolve) => {
    window.speechSynthesis.cancel();

    const utterance = new SpeechSynthesisUtterance(text);
    utterance.rate = 1.0;
    utterance.pitch = 1.0;
    utterance.volume = 1.0;

    const voices = window.speechSynthesis.getVoices();
    const englishVoice = voices.find(v => v.lang.startsWith('en')) || voices[0];
    if (englishVoice) {
      utterance.voice = englishVoice;
    }

    let done = false;
    const finish = () => {
      if (!done) {
        done = true;
        resolve();
      }
    };

    utterance.onend = finish;
    utterance.onerror = (e) => {
      donationElement.innerHTML = '<div style="color: red; font-size: 24px;">TTS SPEECH ERROR: ' + (e.error || 'Unknown failure') + '</div>';
      donationElement.classList.add("show");
      finish();
    };

    setTimeout(() => {
      finish();
    }, 15000);

    try {
      window.speechSynthesis.speak(utterance);
    } catch (err) {
      donationElement.innerHTML = '<div style="color: red; font-size: 24px;">TTS EXCEPTION: ' + err.message + '</div>';
      donationElement.classList.add("show");
      finish();
    }
  });
}
  
async function playDonation(donation) {
  const amount = Number(donation.amount);

  if (!Number.isFinite(amount) || amount < CONFIG.minAmount) {
    tellWorkerReady();
    return;
  }

  const donorName = String(donation.donorName || "Someone");
  const message = String(donation.message || "").slice(0, CONFIG.maxLength);

  const ttsAnnouncement = donorName + " donated " + amount + " Robux using Summer Donations" + (message.length > 0 ? ". " + message : "");

  let htmlContent = '<div class="donation-header">';
  htmlContent += '<span class="donor-name">' + escapeHTML(donorName) + '</span> ';
  htmlContent += '<span class="action-text">donated</span> ';
  htmlContent += '<span class="amount-text">' + amount + ' Robux</span>';
  htmlContent += '</div>';

  if (message.length > 0) {
    htmlContent += '<div class="donation-message">' + escapeHTML(message) + '</div>';
  }

  donationElement.innerHTML = htmlContent;
  donationElement.classList.add("show");

  const minDisplayPromise = new Promise(resolve => setTimeout(resolve, 6000));
  const ttsPromise = speak(ttsAnnouncement);

  await Promise.all([minDisplayPromise, ttsPromise]);

  donationElement.classList.remove("show");

  await new Promise(resolve => setTimeout(resolve, 500));

  tellWorkerReady();
}

function tellWorkerReady() {
  if (socket && socket.readyState === WebSocket.OPEN) {
    socket.send("ready");
  }
}

connect();
<\/script>

</body>
</html>`;
}
__name(createOverlayHTML, "createOverlayHTML");

function json(data, status = 200) {
  return new Response(
    JSON.stringify(data),
    {
      status,
      headers: {
        "Content-Type": "application/json; charset=UTF-8"
      }
    }
  );
}
__name(json, "json");

function cleanText(value, maxLength) {
  if (typeof value !== "string") {
    return "";
  }
  return value.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, "").trim().slice(0, maxLength);
}
__name(cleanText, "cleanText");

function parseBoolean(value, fallback) {
  if (value === null) {
    return fallback;
  }
  return value.toLowerCase() === "true";
}
__name(parseBoolean, "parseBoolean");

function clampNumber(value, min, max, fallback) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return fallback;
  }
  return Math.min(max, Math.max(min, number));
}
__name(clampNumber, "clampNumber");

function safeEqual(a, b) {
  if (typeof a !== "string" || typeof b !== "string") {
    return false;
  }
  const encoder = new TextEncoder();
  const aa = encoder.encode(a);
  const bb = encoder.encode(b);
  if (aa.length !== bb.length) {
    return false;
  }
  let result = 0;
  for (let i = 0; i < aa.length; i++) {
    result |= aa[i] ^ bb[i];
  }
  return result === 0;
}
__name(safeEqual, "safeEqual");

export {
  DonationQueue,
  index_default as default
};