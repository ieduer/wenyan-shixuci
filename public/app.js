const state = {
  bootstrap: null,
  mode: "warmup",
  kind: "",
  run: null,
  currentItem: null,
  selectedLabel: "",
  selectedKeys: new Set(),
  answered: false,
  latestFeedback: null,
  latestReport: null,
};

const els = {
  sessionAlias: document.querySelector("#session-alias"),
  authState: document.querySelector("#auth-state"),
  functionCount: document.querySelector("#function-count"),
  contentCount: document.querySelector("#content-count"),
  questionRoot: document.querySelector("#question-root"),
  submitAnswer: document.querySelector("#submit-answer"),
  nextQuestion: document.querySelector("#next-question"),
  finalizeReport: document.querySelector("#finalize-report"),
  feedbackRoot: document.querySelector("#feedback-root"),
  answeredCount: document.querySelector("#answered-count"),
  correctCount: document.querySelector("#correct-count"),
  streakCount: document.querySelector("#streak-count"),
  reviewCount: document.querySelector("#review-count"),
  badgeStrip: document.querySelector("#badge-strip"),
  leaderboardRoot: document.querySelector("#leaderboard-root"),
  reportRoot: document.querySelector("#report-root"),
  stageKicker: document.querySelector("#stage-kicker"),
  stageTitle: document.querySelector("#stage-title"),
  scoreLabel: document.querySelector("#score-label"),
  scoreFill: document.querySelector("#score-fill"),
};

document.querySelectorAll("[data-mode]").forEach((button) => {
  button.addEventListener("click", () => {
    state.mode = button.dataset.mode || "warmup";
    document.querySelectorAll("[data-mode]").forEach((chip) => chip.classList.remove("active"));
    button.classList.add("active");
    renderModeHint();
  });
});

document.querySelectorAll("[data-kind]").forEach((button) => {
  button.addEventListener("click", () => startChallenge(button.dataset.kind || ""));
});

els.submitAnswer.addEventListener("click", submitAnswer);
els.nextQuestion.addEventListener("click", loadNextQuestion);
els.finalizeReport.addEventListener("click", finalizeReport);

boot().catch((error) => {
  console.error(error);
  renderFeedback({
    correct: false,
    encouragingFeedback: "初始化失敗，請稍後重試。",
    explanation: error.message || String(error),
    pendingReviewCount: 0,
  });
});

async function boot() {
  if (window.BdfzIdentity?.ensureWidget) {
    window.BdfzIdentity.ensureWidget({ siteKey: "wy" });
  }
  await refreshBootstrap();
  renderModeHint();
}

async function refreshBootstrap() {
  const payload = await api("/api/bootstrap");
  state.bootstrap = payload;
  els.sessionAlias.textContent = payload.session.displayName;
  els.functionCount.textContent = String(payload.stats.functionChallenges);
  els.contentCount.textContent = String(payload.stats.contentChallenges);
  if (payload.auth?.authenticated) {
    els.authState.innerHTML = `<span class="auth-strong">${payload.auth.user.slug}</span>`;
  } else {
    els.authState.innerHTML = `<span class="auth-soft">游客学习</span>`;
  }
  renderLeaderboard(payload.leaderboard);
  renderReport();
}

function renderModeHint() {
  const modeLabels = {
    warmup: "熱身模式",
    ranked: "排位模式",
    review: "錯題追擊",
  };
  els.stageKicker.textContent = modeLabels[state.mode];
  if (!state.currentItem) {
    els.stageTitle.textContent =
      state.mode === "review"
        ? "先挑一類進場，系統會優先抓待複習錯題"
        : "選擇實詞或虛詞開始本輪挑戰";
  }
}

async function startChallenge(kind) {
  state.kind = kind;
  state.run = null;
  state.currentItem = null;
  state.selectedLabel = "";
  state.selectedKeys = new Set();
  state.answered = false;
  state.latestReport = null;
  renderReport();
  await loadNextQuestion();
}

async function loadNextQuestion() {
  if (!state.kind) return;
  const search = new URLSearchParams({
    kind: state.kind,
    mode: state.mode,
  });
  if (state.run?.id) {
    search.set("runId", state.run.id);
  }
  const payload = await api(`/api/challenge/next?${search.toString()}`);
  state.run = payload.run;
  state.currentItem = payload.item;
  state.selectedLabel = "";
  state.selectedKeys = new Set();
  state.answered = false;
  els.submitAnswer.disabled = false;
  els.nextQuestion.disabled = true;
  els.finalizeReport.disabled = false;
  renderRun();
  renderQuestion();
  renderFeedback(null);
}

function renderRun() {
  if (!state.run) return;
  const labels = {
    function_word: "虛詞挑戰",
    content_word: "實詞挑戰",
  };
  els.stageTitle.textContent = labels[state.run.kind] || "挑戰進行中";
  els.scoreLabel.textContent = `${Math.max(0, Math.round(state.run.score || 0))} 分`;
  els.scoreFill.style.width = `${Math.min(100, Math.max(0, state.run.answeredCount * 11))}%`;
  els.answeredCount.textContent = String(state.run.answeredCount || 0);
  els.correctCount.textContent = String(state.run.correctCount || 0);
  els.streakCount.textContent = String(state.run.streak || 0);
}

function renderQuestion() {
  if (!state.currentItem?.prompt) {
    els.questionRoot.innerHTML = `<div class="empty-state"><p>暫時沒有可用題目。</p></div>`;
    return;
  }
  const prompt = state.currentItem.prompt;
  const helperLabel = {
    xuci_pair_compare: "北京卷虛詞對照題",
    content_gloss: "實詞義項判定",
    translation_keypoint: "翻譯關鍵點",
    sentence_meaning: "句意落點",
    passage_meaning: "文意判斷",
    analysis_short: "分析短答要點",
  };
  const top = [`<div class="question-block">`, `<p class="stage-kicker">${helperLabel[prompt.questionType] || prompt.questionType}</p>`, `<h3>${escapeHtml(prompt.stem)}</h3>`];
  if (prompt.sentence) {
    top.push(`<div class="question-context">${escapeHtml(prompt.sentence)}</div>`);
  }
  if (prompt.passage) {
    top.push(`<div class="question-passage">${escapeHtml(prompt.passage)}</div>`);
  }
  top.push(`<div class="option-list">`);
  prompt.options.forEach((option) => {
    const selected =
      prompt.responseMode === "multi_select"
        ? state.selectedKeys.has(option.key)
        : state.selectedLabel && state.selectedLabel === option.label;
    const classNames = ["option-card"];
    if (selected) classNames.push("selected");
    const headTag = option.label || option.key || "";
    if (prompt.questionType === "xuci_pair_compare") {
      const pair = Array.isArray(option.sentences)
        ? option.sentences.map((sentence) => `<div class="pair-sentence">${escapeHtml(sentence)}</div>`).join("")
        : "";
      top.push(`
        <button class="${classNames.join(" ")}" type="button" data-option="${headTag}">
          <div class="option-head">
            <span class="option-tag">${escapeHtml(headTag)}</span>
            <strong>${escapeHtml(option.headword || "")}</strong>
          </div>
          <div class="pair-sentences">${pair}</div>
        </button>
      `);
      return;
    }
    if (prompt.responseMode === "multi_select") {
      top.push(`
        <button class="${classNames.join(" ")}" type="button" data-option="${headTag}">
          <div class="multi-option">
            <span class="option-tag">${escapeHtml(headTag)}</span>
            <span>${escapeHtml(option.text || "")}</span>
          </div>
        </button>
      `);
      return;
    }
    top.push(`
      <button class="${classNames.join(" ")}" type="button" data-option="${headTag}">
        <div class="option-head">
          <span class="option-tag">${escapeHtml(headTag)}</span>
        </div>
        <p>${escapeHtml(option.text || "")}</p>
      </button>
    `);
  });
  top.push(`</div></div>`);
  els.questionRoot.innerHTML = top.join("");
  els.questionRoot.querySelectorAll("[data-option]").forEach((button) => {
    button.addEventListener("click", () => selectOption(button.dataset.option || ""));
  });
}

function selectOption(value) {
  const prompt = state.currentItem?.prompt;
  if (!prompt || state.answered) return;
  if (prompt.responseMode === "multi_select") {
    if (state.selectedKeys.has(value)) {
      state.selectedKeys.delete(value);
    } else {
      state.selectedKeys.add(value);
    }
  } else {
    state.selectedLabel = value;
  }
  renderQuestion();
}

async function submitAnswer() {
  if (!state.currentItem?.answerToken || state.answered) return;
  const prompt = state.currentItem.prompt;
  const answer =
    prompt.responseMode === "multi_select"
      ? { keys: [...state.selectedKeys] }
      : { label: state.selectedLabel };
  if ((prompt.responseMode === "multi_select" && !answer.keys.length) || (prompt.responseMode !== "multi_select" && !answer.label)) {
    renderFeedback({
      correct: false,
      encouragingFeedback: "先選答案再提交。",
      explanation: "本題還沒有收到你的作答。",
      pendingReviewCount: Number(els.reviewCount.textContent || 0),
    });
    return;
  }
  const payload = await api("/api/challenge/answer", {
    method: "POST",
    body: JSON.stringify({
      answerToken: state.currentItem.answerToken,
      answer,
    }),
  });
  state.run = payload.run;
  state.answered = true;
  state.latestFeedback = payload.result;
  els.submitAnswer.disabled = true;
  els.nextQuestion.disabled = false;
  els.finalizeReport.disabled = false;
  renderRun();
  renderFeedback(payload.result, payload.badges || []);
}

function renderFeedback(result, badges = []) {
  if (!result) {
    els.feedbackRoot.innerHTML = `<p>提交答案後，這裡會即時告訴你哪裡穩、哪裡要追擊。</p>`;
    return;
  }
  els.reviewCount.textContent = String(result.pendingReviewCount || 0);
  const className = result.correct ? "good" : "bad";
  els.feedbackRoot.innerHTML = `
    <div class="feedback-pill ${className}">
      <strong>${escapeHtml(result.encouragingFeedback || "")}</strong>
      <p>${escapeHtml(result.explanation || "")}</p>
    </div>
  `;
  badges.forEach((badge) => {
    const chip = document.createElement("span");
    chip.className = "badge-chip";
    chip.textContent = `${badge.title}`;
    els.badgeStrip.appendChild(chip);
  });
}

async function finalizeReport() {
  if (!state.run?.id) return;
  const payload = await api("/api/report/finalize", {
    method: "POST",
    body: JSON.stringify({ runId: state.run.id }),
  });
  state.latestReport = payload;
  renderReport();
}

function renderReport() {
  if (!state.latestReport) {
    els.reportRoot.innerHTML = `<p>報告會生成 <code>report.md</code> 和 <code>report.json</code>。登入後，下載記錄會同步回用戶中心。</p>`;
    return;
  }
  const summary = state.latestReport.summary;
  els.reportRoot.innerHTML = `
    <div class="report-meta">
      <div>得分：<strong>${Math.round(summary.score || 0)}</strong></div>
      <div>正確率：<strong>${Math.round((summary.accuracy || 0) * 100)}%</strong></div>
      <div>錯題數：<strong>${(summary.mistake_terms || []).length}</strong></div>
    </div>
    <div class="report-download">
      <span>Markdown 報告</span>
      <a href="${state.latestReport.markdownUrl}" download>下載</a>
    </div>
    <div class="report-download">
      <span>JSON 報告</span>
      <a href="${state.latestReport.jsonUrl}" download>下載</a>
    </div>
  `;
}

function renderLeaderboard(payload) {
  const entries = payload?.entries || [];
  if (!entries.length) {
    els.leaderboardRoot.innerHTML = `<p>目前還沒有可展示的榜單記錄。</p>`;
    return;
  }
  els.leaderboardRoot.innerHTML = entries
    .map(
      (entry) => `
        <div class="leaderboard-entry">
          <span>#${entry.rank} ${escapeHtml(entry.displayName)}</span>
          <strong>${Math.round(entry.score)}</strong>
        </div>
      `
    )
    .join("");
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    credentials: "include",
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.error || `HTTP ${response.status}`);
  }
  return payload;
}

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}
