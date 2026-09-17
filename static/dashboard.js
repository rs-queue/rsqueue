(() => {
  "use strict";

  const $ = (id) => document.getElementById(id);
  const format = (value) => Number(value ?? 0).toLocaleString();
  const escapeHTML = (value) =>
    String(value ?? "").replace(
      /[&<>"']/g,
      (char) =>
        ({
          "&": "&amp;",
          "<": "&lt;",
          ">": "&gt;",
          '"': "&quot;",
          "'": "&#39;",
        })[char],
    );
  const icon = (name) =>
    `<svg class="icon" aria-hidden="true"><use href="#i-${name}"/></svg>`;
  const queuePath = (name) => `/queues/${encodeURIComponent(name)}`;
  const state = {
    queues: [],
    loaded: false,
    connected: false,
    eventSource: null,
    events: [],
    received: 0,
    paused: false,
    pausedEvents: [],
    flow: new Map(),
    refreshPromise: null,
    refreshAgain: false,
    refreshTimer: null,
    renderTimer: null,
    notificationTimer: null,
    dialog: null,
    submitting: false,
  };
  const views = {
    overview: [
      "Overview",
      "A clear view of your queues and the messages moving through them.",
    ],
    queues: [
      "Queues",
      "Manage message queues, watch capacity, and keep work moving.",
    ],
    events: [
      "Event stream",
      "Follow queue activity as it happens, one event at a time.",
    ],
  };

  async function request(path, options = {}) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);
    try {
      const response = await fetch(path, {
        ...options,
        signal: controller.signal,
        headers: {
          ...(options.body ? { "Content-Type": "application/json" } : {}),
          ...options.headers,
        },
      });
      const text = await response.text();
      let body;
      try {
        body = text ? JSON.parse(text) : null;
      } catch {
        body = null;
      }
      if (!response.ok) {
        const messages = {
          401: "Authentication required. Sign in to this server and reload the dashboard.",
          403: "This server did not allow the request.",
          404: "This queue no longer exists. Refresh the queue list.",
          409: "A queue with this name already exists. Choose another name.",
        };
        throw new Error(
          body?.error ||
            messages[response.status] ||
            `Request failed (${response.status}). Please try again.`,
        );
      }
      if (body?.error) throw new Error(body.error);
      return body;
    } catch (error) {
      if (error.name === "AbortError")
        throw new Error(
          "The server took too long to respond. Please try again.",
        );
      if (error instanceof TypeError)
        throw new Error(
          "Cannot reach the server. Check your connection and try again.",
        );
      throw error;
    } finally {
      clearTimeout(timeout);
    }
  }

  function setView(focus = false) {
    const view = location.hash.slice(1) || "overview";
    if (!Object.hasOwn(views, view)) return;
    document.querySelectorAll(".view").forEach((section) => {
      section.hidden = section.id !== view;
    });
    document.querySelectorAll("[data-view]").forEach((link) => {
      const active = link.dataset.view === view;
      link.classList.toggle("active", active);
      if (active) link.setAttribute("aria-current", "page");
      else link.removeAttribute("aria-current");
    });
    $("pageTitle").textContent = $("breadcrumbTitle").textContent =
      views[view][0];
    $("pageDescription").textContent = views[view][1];
    document.title = `${views[view][0]} · RSQueue`;
    if (focus) $("main").focus({ preventScroll: true });
  }

  function updateConnection(connected) {
    state.connected = connected;
    $("statusIndicator").className =
      `status-dot ${connected ? "connected" : "disconnected"}`;
    $("footerStatusDot").className =
      `small-dot ${connected ? "connected" : "disconnected"}`;
    $("statusText").textContent = connected ? "Live updates" : "Reconnecting";
    $("footerStatus").textContent = connected
      ? "Connected to your instance"
      : "Live updates interrupted · reconnecting automatically";
    renderChart();
  }

  function connect() {
    state.eventSource?.close();
    const source = new EventSource("/events");
    state.eventSource = source;
    source.onopen = () => {
      updateConnection(true);
      refresh();
    };
    source.onerror = () => updateConnection(false);
    source.onmessage = (message) => {
      let event;
      try {
        event = JSON.parse(message.data);
      } catch {
        return;
      }
      if (!event || typeof event.type !== "string") return;
      if (event.type === "heartbeat" || event.type === "metrics_update") return;
      state.received++;
      state.events.unshift({ ...event, receivedAt: Date.now() });
      state.events.length = Math.min(state.events.length, 100);
      if (
        event.type === "message_enqueued" ||
        event.type === "messages_dequeued"
      ) {
        const bucket = currentBucket();
        bucket[event.type === "message_enqueued" ? "incoming" : "outgoing"] +=
          event.count ?? 1;
      }
      // Zero is a valid queue depth. Never keep the previous value when a queue empties.
      const queue = state.queues.find((item) => item.name === event.queue_name);
      if (queue && Number.isFinite(event.queue_depth))
        queue.size = event.queue_depth;
      if (queue && event.type === "queue_purged") queue.size = 0;
      if (queue && event.type === "messages_evicted")
        queue.max_queue_size = event.max_queue_size;
      // Coalesce bursts of messages so the event stream stays responsive.
      if (!state.renderTimer) {
        state.renderTimer = setTimeout(() => {
          state.renderTimer = null;
          renderQueues();
          renderEvents();
        }, 100);
      }
      scheduleRefresh();
    };
  }

  function scheduleRefresh() {
    if (state.refreshTimer) return;
    state.refreshTimer = setTimeout(() => {
      state.refreshTimer = null;
      refresh();
    }, 350);
  }

  function refresh() {
    if (state.refreshPromise) {
      state.refreshAgain = true;
      return state.refreshPromise;
    }
    document.querySelectorAll('[data-action="refresh"]').forEach((button) => {
      button.disabled = true;
    });
    state.refreshPromise = (async () => {
      try {
        // Server counters are authoritative, including after a reconnect or page reload.
        const [queues, metrics] = await Promise.all([
          request("/queues"),
          request("/metrics/summary"),
        ]);
        if (!Array.isArray(queues) || !metrics)
          throw new Error(
            "The server returned an unexpected response. Please retry.",
          );
        state.queues = queues.sort((a, b) => a.name.localeCompare(b.name));
        state.loaded = true;
        $("activeQueues").textContent = format(queues.length);
        const fields = {
          totalPending: "total_messages_pending",
          totalInFlight: "total_messages_in_flight",
          totalEnqueued: "messages_enqueued_total",
          totalDequeued: "messages_dequeued_total",
          totalDeleted: "messages_deleted_total",
          totalEvicted: "messages_evicted_total",
        };
        for (const [id, field] of Object.entries(fields))
          $(id).textContent = format(metrics[field]);
        $("totalEvicted").classList.toggle(
          "has-warning",
          metrics.messages_evicted_total > 0,
        );
        $("dataError").hidden = true;
        $("lastUpdated").textContent =
          `Updated ${new Date().toLocaleTimeString()}`;
        renderQueues();
      } catch (error) {
        $("dataErrorText").textContent =
          `${error.message}${state.loaded ? " Showing the last available data." : ""}`;
        $("dataError").hidden = false;
        if (!state.loaded) {
          const message = emptyState(
            "stack",
            "Unable to load queues",
            "Use Retry above to reconnect to your server.",
          );
          $("overviewQueues").innerHTML = $("queuesContainer").innerHTML =
            message;
          $("queueResults").textContent = "Queue data unavailable";
        }
      } finally {
        state.refreshPromise = null;
        document
          .querySelectorAll('[data-action="refresh"]')
          .forEach((button) => {
            button.disabled = false;
          });
        if (state.refreshAgain) {
          state.refreshAgain = false;
          scheduleRefresh();
        }
      }
    })();
    return state.refreshPromise;
  }

  function emptyState(symbol, title, description, action = "") {
    return `<div class="empty-state">${icon(symbol)}<strong>${escapeHTML(title)}</strong><p>${escapeHTML(description)}</p>${action}</div>`;
  }

  function capacity(queue) {
    const cap = queue.max_queue_size;
    if (!cap) return '<span class="capacity-unlimited">No size cap</span>';
    const percent = Math.min(100, Math.round((queue.size / cap) * 100));
    const tone = percent >= 100 ? "danger" : percent >= 80 ? "warning" : "";
    return `<div class="capacity"><div class="capacity-top"><span>${format(cap)} max</span><span>${percent}%</span></div><div class="capacity-track" role="meter" aria-label="Queue capacity used" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${percent}"><div class="capacity-fill ${tone}" style="width:${percent}%"></div></div></div>`;
  }

  function queueTable(queues, overview = false) {
    const rows = queues
      .map((queue) => {
        const name = escapeHTML(queue.name);
        const full =
          queue.max_queue_size > 0 && queue.size >= queue.max_queue_size;
        const status = full
          ? ["full", "At capacity"]
          : queue.size > 0
            ? ["active", "Has messages"]
            : ["idle", "Empty"];
        return `<tr><td><div class="queue-name" title="${name}">${icon("stack")}${overview ? `<a href="#queues" data-queue-search="${name}">${name}</a>` : `<span>${name}</span>`}</div>${overview ? "" : `<div class="queue-detail">${format(queue.visibility_timeout_seconds)}s visibility timeout</div>`}</td>
                <td class="number-cell">${format(queue.size)}</td><td>${capacity(queue)}</td><td class="status-column"><span class="status-badge ${status[0]}">${status[1]}</span></td>
                ${overview ? "" : `<td><div class="row-actions"><button class="button secondary" data-action="send" data-queue="${name}" aria-label="Send message to ${name}">${icon("plus")}Send message</button><button class="icon-button" data-action="limit" data-queue="${name}" aria-label="Set limit for ${name}" title="Set size cap">${icon("settings")}</button><button class="icon-button delete" data-action="delete" data-queue="${name}" aria-label="Delete queue ${name}" title="Delete queue">${icon("trash")}</button></div></td>`}</tr>`;
      })
      .join("");
    return `<table aria-label="${overview ? "Queue overview" : "Queues"}"><thead><tr><th scope="col">Queue name</th><th scope="col" class="number-cell">Messages</th><th scope="col">Capacity</th><th scope="col" class="status-column">Status</th>${overview ? "" : '<th scope="col" class="number-cell">Actions</th>'}</tr></thead><tbody>${rows}</tbody></table>`;
  }

  function replaceContent(id, html) {
    const container = $(id);
    if (container.dataset.rendered === html) return;
    const active = container.contains(document.activeElement)
      ? document.activeElement
      : null;
    const action = active?.dataset.action;
    const name = active?.dataset.queue;
    container.innerHTML = html;
    container.dataset.rendered = html;
    // A live queue update should not interrupt someone using the keyboard.
    if (action)
      [...container.querySelectorAll("[data-action]")]
        .find(
          (button) =>
            button.dataset.action === action && button.dataset.queue === name,
        )
        ?.focus({ preventScroll: true });
  }

  function renderQueues() {
    if (!state.loaded) return;
    const count = state.queues.length;
    $("navQueueCount").textContent = $("overviewQueueCount").textContent =
      format(count);
    const noQueues = emptyState(
      "stack",
      "Your first queue starts here",
      "Create a queue to start sending and processing messages.",
      '<button class="button secondary" data-action="create">Create queue</button>',
    );
    replaceContent(
      "overviewQueues",
      count ? queueTable(state.queues.slice(0, 5), true) : noQueues,
    );
    const query = $("queueSearch").value.trim().toLowerCase();
    const filter = $("queueFilter").value;
    const filtered = state.queues.filter(
      (queue) =>
        queue.name.toLowerCase().includes(query) &&
        (filter !== "bounded" || queue.max_queue_size > 0) &&
        (filter !== "full" ||
          (queue.max_queue_size > 0 && queue.size >= queue.max_queue_size)),
    );
    replaceContent(
      "queuesContainer",
      !count
        ? noQueues
        : filtered.length
          ? queueTable(filtered)
          : emptyState(
              "search",
              "No matching queues",
              "Try another name or change the filter.",
              '<button class="text-button" data-action="reset-search">Clear filters</button>',
            ),
    );
    $("queueResults").textContent =
      `${format(filtered.length)} of ${format(count)} ${count === 1 ? "queue" : "queues"}`;
  }

  function eventDetails(event) {
    const types = {
      message_enqueued: ["enqueue", "Message enqueued"],
      messages_dequeued: [
        "dequeue",
        `${format(event.count)} messages dequeued`,
      ],
      message_deleted: ["deleted", "Message acknowledged"],
      batch_deleted: [
        "deleted",
        `${format(event.successful)} messages deleted${event.failed ? `, ${format(event.failed)} failed` : ""}`,
      ],
      queue_created: ["queue", "Queue created"],
      queue_deleted: ["queue", "Queue deleted"],
      queue_purged: ["queue", "Queue purged"],
      messages_evicted: [
        "evicted",
        `${format(event.count)} messages dropped over cap`,
      ],
      message_moved_to_dlq: ["evicted", "Message moved to dead letter queue"],
    };
    const [tone, title] = types[event.type] || [
      "queue",
      event.type.replaceAll("_", " "),
    ];
    const detail =
      event.type === "message_moved_to_dlq"
        ? `${event.source_queue} → ${event.dlq_queue}`
        : event.queue_name || "Server activity";
    const time = new Date(event.timestamp || event.receivedAt);
    return `<div class="activity-item"><span class="activity-dot ${tone}"></span><div class="activity-body"><strong>${escapeHTML(title)}</strong><p>${escapeHTML(detail)}</p></div><time class="activity-time" title="${escapeHTML(time.toLocaleString())}">${escapeHTML(time.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false }))}</time></div>`;
  }

  function renderEvents() {
    const noEvents = emptyState(
      "activity",
      "Listening for activity",
      "Create a queue or send a message. New events will appear here.",
    );
    $("recentEvents").innerHTML = state.events.length
      ? state.events.slice(0, 4).map(eventDetails).join("")
      : noEvents;
    const events = state.paused ? state.pausedEvents : state.events;
    const filter = $("eventFilter").value;
    const filtered = events.filter(
      (event) =>
        filter === "all" ||
        (filter === "messages" && !event.type.startsWith("queue_")) ||
        (filter === "queues" && event.type.startsWith("queue_")) ||
        (filter === "evicted" && event.type === "messages_evicted"),
    );
    $("eventsList").innerHTML = filtered.length
      ? filtered.map(eventDetails).join("")
      : filter === "all"
        ? noEvents
        : emptyState(
            "search",
            "No matching events",
            "Select another event type or wait for new activity.",
          );
    $("eventCount").textContent = format(filtered.length);
    $("eventsReceived").textContent =
      `${format(state.received)} events received`;
  }

  function currentBucket() {
    const second = Math.floor(Date.now() / 1000);
    if (!state.flow.has(second))
      state.flow.set(second, { incoming: 0, outgoing: 0 });
    return state.flow.get(second);
  }

  function renderChart() {
    const now = Math.floor(Date.now() / 1000);
    if (state.connected) currentBucket();
    for (const second of state.flow.keys())
      if (second < now - 59) state.flow.delete(second);
    const points = Array.from({ length: 60 }, (_, index) =>
      state.flow.get(now - 59 + index),
    );
    const max = Math.max(
      4,
      ...points.flatMap((point) =>
        point ? [point.incoming, point.outgoing] : [],
      ),
    );
    const ceiling = Math.ceil(max / 4) * 4;
    const grid = Array.from({ length: 5 }, (_, index) => {
      const y = 10 + index * 47;
      return `<line x1="34" x2="718" y1="${y}" y2="${y}" stroke="#edf0f5" stroke-dasharray="3 4"/><text x="0" y="${y + 4}" fill="#687287" font-size="11">${format((ceiling * (4 - index)) / 4)}</text>`;
    }).join("");
    const line = (key, color) => {
      let drawing = false;
      const path = points
        .map((point, index) => {
          if (!point) {
            drawing = false;
            return "";
          }
          const command = drawing ? "L" : "M";
          drawing = true;
          return `${command}${(34 + (index * 684) / 59).toFixed(1)},${(198 - (point[key] / ceiling) * 188).toFixed(1)}`;
        })
        .join(" ");
      return `<path d="${path}" fill="none" stroke="${color}" stroke-width="2.5" stroke-linejoin="round" vector-effect="non-scaling-stroke"/>`;
    };
    $("flowChart").innerHTML =
      grid + line("incoming", "#6654d9") + line("outgoing", "#237e70");
    const last = state.flow.get(now - 1);
    $("incomingRate").textContent = last ? format(last.incoming) : "—";
    $("outgoingRate").textContent = last ? format(last.outgoing) : "—";
    const total = points.reduce(
      (sum, point) => sum + (point ? point.incoming + point.outgoing : 0),
      0,
    );
    $("chartEmpty").hidden = total > 0;
    $("chartEmpty").querySelector("strong").textContent = state.connected
      ? "Listening for traffic"
      : "Waiting for connection";
    $("chartEmpty").querySelector("span:last-child").textContent =
      state.connected
        ? "Send a message to see activity here."
        : "Traffic will resume when the server reconnects.";
    $("flowChart").setAttribute(
      "aria-label",
      `Message traffic over the last 60 seconds: ${format(total)} message operations observed. Gaps indicate no connection.`,
    );
  }

  function numberField(
    name,
    label,
    value,
    help,
    min = 0,
    max = Number.MAX_SAFE_INTEGER,
  ) {
    return `<label class="form-field"><span id="field-${name}-label">${label}</span><input name="${name}" type="number" min="${min}" max="${max}" step="1" value="${value}" aria-labelledby="field-${name}-label" ${help ? `aria-describedby="field-${name}-help"` : ""} required>${help ? `<small id="field-${name}-help">${help}</small>` : ""}</label>`;
  }

  function openDialog(mode, name = "") {
    if (state.submitting) return;
    const queue = state.queues.find((item) => item.name === name);
    if (mode !== "create" && !queue) {
      refresh();
      return;
    }
    state.dialog = { mode, name, trigger: document.activeElement };
    const content = {
      create: [
        "Create queue",
        "Give your queue a name and choose how many messages it can hold.",
        '<label class="form-field"><span id="field-name-label">Queue name</span><input name="name" placeholder="e.g. order-processing" maxlength="120" pattern="[a-zA-Z0-9][a-zA-Z0-9._\\-]*" title="Start with a letter or number. Use letters, numbers, dots, hyphens, or underscores." aria-labelledby="field-name-label" aria-describedby="field-name-help" autocomplete="off" required><small id="field-name-help">Use letters, numbers, dots, hyphens, or underscores.</small></label>' +
          numberField(
            "maxSize",
            "Maximum messages",
            0,
            "Use 0 for an unlimited queue.",
          ) +
          numberField(
            "visibility",
            "Visibility timeout (seconds)",
            120,
            "Time a consumer has to acknowledge a message.",
            1,
          ),
      ],
      limit: [
        "Set queue limit",
        `Set the maximum number of messages in “${name}”.`,
        numberField(
          "maxSize",
          "Maximum messages",
          queue?.max_queue_size || 0,
          "Includes pending and in-flight messages. Use 0 for unlimited.",
        ) +
          '<p class="form-warning">Lowering the cap can immediately drop messages. The oldest messages with the lowest priority are removed first.</p>',
      ],
      send: [
        "Send message",
        `Add a message to “${name}”.`,
        '<label class="form-field"><span>Message content</span><textarea name="content" placeholder="Enter a message or JSON payload…" required></textarea></label>' +
          '<div class="form-row">' +
          numberField(
            "priority",
            "Priority",
            0,
            "0–9. Higher goes first.",
            0,
            9,
          ) +
          numberField("delay", "Delay (seconds)", 0, "0 sends immediately.") +
          "</div>",
      ],
      delete: [
        "Delete queue",
        `Permanently delete “${name}” and all ${format(queue?.size)} of its messages?`,
        '<p class="form-warning">This action cannot be undone. Consumers will no longer be able to access this queue.</p>',
      ],
    }[mode];
    $("dialogTitle").textContent = content[0];
    $("dialogDescription").textContent = content[1];
    $("dialogFields").innerHTML = content[2];
    $("dialogError").hidden = true;
    $("dialogSubmit").textContent =
      mode === "limit" ? "Save limit" : content[0];
    $("dialogSubmit").className =
      `button ${mode === "delete" ? "danger" : "primary"}`;
    $("queueDialog").showModal();
    if (mode === "delete")
      $("queueDialog")
        .querySelector('.dialog-footer [data-action="close-dialog"]')
        .focus();
    else $("dialogFields").querySelector("input, textarea")?.focus();
  }

  function closeDialog() {
    if (state.submitting) return;
    const trigger = state.dialog?.trigger;
    $("queueDialog").close();
    if (trigger?.isConnected && state.dialog?.mode !== "delete")
      trigger.focus({ preventScroll: true });
    else
      document
        .querySelector('.page-heading [data-action="create"]')
        .focus({ preventScroll: true });
    state.dialog = null;
  }

  function wholeNumber(data, name) {
    const raw = data.get(name);
    const value = Number(raw);
    if (!/^\d+$/.test(raw) || !Number.isSafeInteger(value) || value < 0)
      throw new Error("Enter a valid whole number.");
    return value;
  }

  async function submitDialog(event) {
    event.preventDefault();
    if (state.submitting) return;
    const { mode, name } = state.dialog;
    const data = new FormData($("queueForm"));
    const submit = $("dialogSubmit");
    const label = submit.textContent;
    $("dialogError").hidden = true;
    try {
      let path = queuePath(name);
      let method = "POST";
      let body;
      let success;
      if (mode === "create") {
        const queueName = data.get("name").trim();
        body = {
          name: queueName,
          max_queue_size: wholeNumber(data, "maxSize"),
          visibility_timeout_seconds: wholeNumber(data, "visibility"),
        };
        path = "/queues";
        success = `Queue “${queueName}” created`;
      } else if (mode === "limit") {
        path += "/settings";
        method = "PUT";
        body = { max_queue_size: wholeNumber(data, "maxSize") };
        success = `Size cap updated for “${name}”`;
      } else if (mode === "send") {
        path += "/messages";
        if (!data.get("content").trim())
          throw new Error("Enter some message content.");
        body = {
          content: data.get("content"),
          priority: wholeNumber(data, "priority"),
          delay_seconds: wholeNumber(data, "delay"),
        };
        success = `Message sent to “${name}”`;
      } else {
        method = "DELETE";
        success = `Queue “${name}” deleted`;
      }
      state.submitting = true;
      submit.disabled = true;
      submit.textContent = "Saving…";
      $("queueDialog")
        .querySelectorAll('[data-action="close-dialog"]')
        .forEach((button) => {
          button.disabled = true;
        });
      await request(path, {
        method,
        ...(body ? { body: JSON.stringify(body) } : {}),
      });
      if (mode === "create") {
        $("queueSearch").value = "";
        $("queueFilter").value = "all";
        location.hash = "queues";
      }
      state.submitting = false;
      closeDialog();
      notify(success);
      refresh();
    } catch (error) {
      $("dialogError").textContent = error.message;
      $("dialogError").hidden = false;
    } finally {
      state.submitting = false;
      submit.disabled = false;
      submit.textContent = label;
      $("queueDialog")
        .querySelectorAll('[data-action="close-dialog"]')
        .forEach((button) => {
          button.disabled = false;
        });
    }
  }

  function notify(message) {
    clearTimeout(state.notificationTimer);
    $("notifications").innerHTML =
      `<div class="toast">${icon("check")}<span>${escapeHTML(message)}</span></div>`;
    state.notificationTimer = setTimeout(() => {
      $("notifications").replaceChildren();
    }, 5000);
  }

  document.addEventListener("click", (event) => {
    const search = event.target.closest("[data-queue-search]");
    if (search) {
      $("queueSearch").value = search.dataset.queueSearch;
      $("queueFilter").value = "all";
      renderQueues();
    }
    const button = event.target.closest("[data-action]");
    if (!button || button.disabled) return;
    const action = button.dataset.action;
    if (["create", "limit", "send", "delete"].includes(action))
      openDialog(action, button.dataset.queue);
    if (action === "close-dialog") closeDialog();
    if (action === "refresh") refresh();
    if (action === "reset-search") {
      $("queueSearch").value = "";
      $("queueFilter").value = "all";
      renderQueues();
      $("queueSearch").focus();
    }
  });
  $("queueForm").addEventListener("submit", submitDialog);
  $("queueDialog").addEventListener("cancel", (event) => {
    event.preventDefault();
    closeDialog();
  });
  $("queueSearch").addEventListener("input", renderQueues);
  $("queueFilter").addEventListener("change", renderQueues);
  $("eventFilter").addEventListener("change", renderEvents);
  $("pauseEvents").addEventListener("click", () => {
    state.paused = !state.paused;
    state.pausedEvents = state.paused ? [...state.events] : [];
    $("pauseEvents").textContent = state.paused ? "Resume" : "Pause";
    $("pauseEvents").setAttribute("aria-pressed", String(state.paused));
    $("pauseNotice").hidden = !state.paused;
    renderEvents();
  });
  $("clearEvents").addEventListener("click", () => {
    state.events = [];
    state.pausedEvents = [];
    renderEvents();
  });
  window.addEventListener("hashchange", () => setView(true));
  window.addEventListener("offline", () => {
    state.eventSource?.close();
    updateConnection(false);
  });
  window.addEventListener("online", connect);
  window.addEventListener("pagehide", () => state.eventSource?.close());
  window.addEventListener("pageshow", (event) => {
    if (event.persisted) location.reload();
  });
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) {
      refresh();
      renderChart();
    }
  });
  $("serverHost").textContent = location.host;
  setView();
  renderEvents();
  renderChart();
  refresh();
  connect();
  setInterval(renderChart, 1000);
  setInterval(() => {
    if (!document.hidden) refresh();
  }, 10000);
})();
