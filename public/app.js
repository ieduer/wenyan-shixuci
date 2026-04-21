const THEME_KEY = "wy-theme";
const THEMES = ["parchment", "forest", "ink"];

const state = {
  bootstrap: null,
  mode: "warmup",
  theme: loadTheme(),
  kind: "",
  run: null,
  currentItem: null,
  selectedLabel: "",
  selectedKeys: new Set(),
  answered: false,
  answerReveal: null,
  latestFeedback: null,
  latestBadges: [],
  latestReport: null,
  finalizingRunId: "",
};

const els = {
  questionRoot: document.querySelector("#question-root"),
  submitAnswer: document.querySelector("#submit-answer"),
  nextQuestion: document.querySelector("#next-question"),
  stageNote: document.querySelector("#stage-note"),
  answeredCount: document.querySelector("#answered-count"),
  correctCount: document.querySelector("#correct-count"),
  streakCount: document.querySelector("#streak-count"),
  reviewCount: document.querySelector("#review-count"),
  answeredFill: document.querySelector("#answered-fill"),
  correctFill: document.querySelector("#correct-fill"),
  reviewFill: document.querySelector("#review-fill"),
  badgeStrip: document.querySelector("#badge-strip"),
  leaderboardRoot: document.querySelector("#leaderboard-root"),
  stageKicker: document.querySelector("#stage-kicker"),
  stageTitle: document.querySelector("#stage-title"),
  scoreLabel: document.querySelector("#score-label"),
  scoreFill: document.querySelector("#score-fill"),
  rankName: document.querySelector("#rank-name"),
};

document.querySelectorAll("[data-mode]").forEach((button) => {
  button.addEventListener("click", async () => {
    const nextMode = button.dataset.mode || "warmup";
    if (state.mode === nextMode) return;
    await maybeFinalizeCurrentRun();
    state.mode = nextMode;
    clearRoundState();
    syncModeButtons();
    renderModeHeader();
    renderRun();
    renderQuestion();
    renderStageNote();
  });
});

document.querySelectorAll("[data-kind]").forEach((button) => {
  button.addEventListener("click", () => startChallenge(button.dataset.kind || ""));
});

document.querySelectorAll("[data-theme]").forEach((button) => {
  button.addEventListener("click", () => setTheme(button.dataset.theme || "parchment"));
});

els.submitAnswer.addEventListener("click", submitAnswer);
els.nextQuestion.addEventListener("click", loadNextQuestion);
window.addEventListener("pagehide", finalizeRunWithBeacon);

applyTheme(state.theme);
syncModeButtons();

boot().catch((error) => {
  console.error(error);
  state.latestFeedback = {
    correct: false,
    encouragingFeedback: "初始化失敗",
    explanation: error.message || String(error),
    pendingReviewCount: 0,
  };
  renderStageNote();
});

async function boot() {
  if (window.BdfzIdentity?.ensureWidget) {
    window.BdfzIdentity.ensureWidget({
      siteKey: "wy",
      insetBottom: 20,
      mobileInsetBottom: 84,
      mobileInsetSide: 12,
    });
  }
  await refreshBootstrap();
  renderModeHeader();
  renderRun();
  renderQuestion();
  renderStageNote();
}

async function refreshBootstrap() {
  const payload = await api("/api/bootstrap");
  state.bootstrap = payload;
  syncEntryButtons();
  renderLeaderboard(payload.leaderboard);
}

function clearRoundState() {
  state.run = null;
  state.currentItem = null;
  state.selectedLabel = "";
  state.selectedKeys = new Set();
  state.answered = false;
  state.answerReveal = null;
  state.latestFeedback = null;
  state.latestBadges = [];
}

async function startChallenge(kind) {
  await maybeFinalizeCurrentRun();
  if (!hasAvailableChallenges(kind)) {
    state.kind = kind;
    state.run = null;
    state.currentItem = null;
    state.answerReveal = null;
    state.latestFeedback = {
      correct: false,
      encouragingFeedback: "題庫整理中",
      explanation: `${kindLabel(kind)}目前沒有可驗證的題目可供出題。`,
      pendingReviewCount: 0,
    };
    syncEntryButtons();
    renderModeHeader();
    renderRun();
    renderQuestion();
    renderStageNote();
    return;
  }
  state.kind = kind;
  clearRoundState();
  syncEntryButtons();
  renderModeHeader();
  renderRun();
  renderQuestion();
  renderStageNote();
  await loadNextQuestion();
}

async function loadNextQuestion() {
  if (!state.kind) return;
  try {
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
    state.answerReveal = null;
    state.latestFeedback = null;
    els.submitAnswer.disabled = false;
    els.nextQuestion.disabled = true;
    renderModeHeader();
    renderRun();
    renderQuestion();
    renderStageNote();
  } catch (error) {
    state.latestFeedback = {
      correct: false,
      encouragingFeedback: "抽題失敗",
      explanation: error.message || String(error),
      pendingReviewCount: Number(els.reviewCount.textContent || 0),
    };
    renderStageNote();
  }
}

function renderModeHeader() {
  const modeText = {
    warmup: "熱身",
    ranked: "排位",
    review: "追擊",
  };
  els.stageKicker.textContent = modeText[state.mode];
  if (state.currentItem?.prompt) return;
  if (!state.kind) {
    els.stageTitle.textContent = "選擇實詞或虛詞開始";
    return;
  }
  els.stageTitle.textContent = `${kindLabel(state.kind)}待命`;
}

function renderRun() {
  const run = state.run;
  if (!run) {
    els.scoreLabel.textContent = "0 分";
    els.scoreFill.style.width = "0%";
    els.answeredCount.textContent = "0";
    els.correctCount.textContent = "0";
    els.streakCount.textContent = "0";
    els.reviewCount.textContent = "0";
    els.answeredFill.style.width = "0%";
    els.correctFill.style.width = "0%";
    els.reviewFill.style.width = "0%";
    els.rankName.textContent = "未開局";
    renderBadges([]);
    return;
  }

  const answered = Number(run.answeredCount || 0);
  const correct = Number(run.correctCount || 0);
  const streak = Number(run.streak || 0);
  const review = Number(state.latestFeedback?.pendingReviewCount || 0);
  const score = Math.max(0, Math.round(run.score || 0));
  const accuracy = answered > 0 ? Math.round((correct / answered) * 100) : 0;

  els.stageTitle.textContent = `${kindLabel(run.kind)} · ${modeLabel(run.mode)}`;
  els.scoreLabel.textContent = `${score} 分`;
  els.scoreFill.style.width = `${clampPercent(answered * 11 + accuracy * 0.22)}%`;
  els.answeredCount.textContent = String(answered);
  els.correctCount.textContent = String(correct);
  els.streakCount.textContent = String(streak);
  els.reviewCount.textContent = String(review);
  els.answeredFill.style.width = `${clampPercent(answered * 14)}%`;
  els.correctFill.style.width = `${clampPercent(accuracy)}%`;
  els.reviewFill.style.width = `${clampPercent(review * 24)}%`;
  els.rankName.textContent = deriveRank(score, streak, accuracy);
  renderBadges(state.latestBadges);
}

function renderQuestion() {
  if (!state.currentItem?.prompt) {
    els.questionRoot.innerHTML = `
      <div class="empty-state">
        <p>選一種詞類，直接進場。</p>
      </div>
    `;
    return;
  }

  const prompt = state.currentItem.prompt;
  const headword = termHeadword(prompt.termId);
  const tagLabel = questionTypeLabel(prompt.questionType);
  const modeTag = prompt.responseMode === "multi_select" ? "多選" : "單選";
  const parts = [
    `<div class="question-block">`,
    `<div class="question-tag-row">`,
    `<span class="question-tag">${escapeHtml(kindLabel(prompt.kind))}</span>`,
    `<span class="question-tag">${escapeHtml(tagLabel)}</span>`,
    `<span class="question-tag">${escapeHtml(modeTag)}</span>`,
    `</div>`,
    `<h3>${escapeHtml(prompt.stem)}</h3>`,
  ];

  if (prompt.sentence) {
    parts.push(`<div class="question-context">${highlightTerm(prompt.sentence, headword)}</div>`);
  }
  if (prompt.passage) {
    parts.push(`<div class="question-passage">${highlightTerm(prompt.passage, headword)}</div>`);
  }

  parts.push(`<div class="option-list">`);
  prompt.options.forEach((option) => {
    const key = option.label || option.key || "";
    const status = optionVisualState(prompt, option, key);
    const className = [
      "option-card",
      status.selected ? "selected" : "",
      status.correct ? "correct" : "",
      status.incorrect ? "incorrect" : "",
      status.dimmed ? "dimmed" : "",
    ]
      .filter(Boolean)
      .join(" ");
    const outcomeTag = status.correct
      ? `<span class="option-result option-result-good">命中</span>`
      : status.incorrect
        ? `<span class="option-result option-result-bad">失手</span>`
        : "";

    if (prompt.questionType === "xuci_pair_compare") {
      const pairHtml = Array.isArray(option.sentences)
        ? option.sentences
            .map((sentence) => `<div class="pair-sentence">${highlightTerm(sentence, option.headword || headword)}</div>`)
            .join("")
        : "";
      parts.push(`
        <button class="${className}" type="button" data-option="${escapeAttr(key)}" ${state.answered ? "disabled" : ""}>
          <div class="option-head">
            <span class="option-tag">${escapeHtml(key)}</span>
            <strong>${escapeHtml(option.headword || "")}</strong>
            ${outcomeTag}
          </div>
          <div class="pair-sentences">${pairHtml}</div>
        </button>
      `);
      return;
    }

    if (prompt.responseMode === "multi_select") {
      parts.push(`
        <button class="${className}" type="button" data-option="${escapeAttr(key)}" ${state.answered ? "disabled" : ""}>
          <div class="multi-option">
            <span class="option-tag">${escapeHtml(key)}</span>
            <span>${highlightTerm(option.text || "", headword)}</span>
            ${outcomeTag}
          </div>
        </button>
      `);
      return;
    }

    parts.push(`
      <button class="${className}" type="button" data-option="${escapeAttr(key)}" ${state.answered ? "disabled" : ""}>
        <div class="option-head">
          <span class="option-tag">${escapeHtml(key)}</span>
          ${outcomeTag}
        </div>
        <p>${highlightTerm(option.text || "", headword)}</p>
      </button>
    `);
  });
  parts.push(`</div></div>`);
  els.questionRoot.innerHTML = parts.join("");
  els.questionRoot.querySelectorAll("[data-option]").forEach((button) => {
    button.addEventListener("click", () => selectOption(button.dataset.option || ""));
  });
}

function optionVisualState(prompt, option, key) {
  const optionKey = option.key || key;
  const selected =
    prompt.responseMode === "multi_select"
      ? state.selectedKeys.has(optionKey)
      : state.selectedLabel === option.label;
  if (!state.answered || !state.answerReveal) {
    return { selected, correct: false, incorrect: false, dimmed: false };
  }
  const submitted = state.answerReveal.submitted.has(key);
  const correct = state.answerReveal.correct.has(key);
  return {
    selected: selected || submitted || correct,
    correct,
    incorrect: submitted && !correct,
    dimmed: !submitted && !correct,
  };
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
    state.latestFeedback = {
      correct: false,
      encouragingFeedback: "先選答案",
      explanation: "這題還沒有收到你的作答。",
      pendingReviewCount: Number(els.reviewCount.textContent || 0),
    };
    renderStageNote();
    return;
  }

  try {
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
    state.answerReveal = buildAnswerReveal(payload.result.correctAnswer, payload.result.submittedAnswer);
    state.latestBadges = mergeBadges(state.latestBadges, payload.badges || []);
    els.submitAnswer.disabled = true;
    els.nextQuestion.disabled = false;
    renderRun();
    renderQuestion();
    renderStageNote();
  } catch (error) {
    state.latestFeedback = {
      correct: false,
      encouragingFeedback: "提交失敗",
      explanation: error.message || String(error),
      pendingReviewCount: Number(els.reviewCount.textContent || 0),
    };
    renderStageNote();
  }
}

function renderStageNote() {
  const result = state.latestFeedback;
  if (!result) {
    els.stageNote.hidden = true;
    els.stageNote.innerHTML = "";
    return;
  }

  const reviewCount = Number(result.pendingReviewCount || 0);
  els.reviewCount.textContent = String(reviewCount);
  els.reviewFill.style.width = `${clampPercent(reviewCount * 24)}%`;

  const title = state.answered ? (result.correct ? "答對" : "加入追擊") : result.encouragingFeedback || "";
  const detail = state.answered
    ? result.correct
      ? `+${Math.round(result.scoreDelta || 0)} 分 · 可直接進入下一題`
      : `本題已進入追擊佇列${reviewCount ? ` · 目前 ${reviewCount} 題待追擊` : ""}`
    : result.explanation || "";
  const tone = result.correct ? "good" : state.answered ? "bad" : "muted";

  els.stageNote.hidden = false;
  els.stageNote.innerHTML = `
    <div class="stage-note-card ${tone}">
      <strong>${escapeHtml(title)}</strong>
      <span>${escapeHtml(detail)}</span>
    </div>
  `;
}

async function maybeFinalizeCurrentRun() {
  if (!state.run?.id || Number(state.run.answeredCount || 0) === 0 || state.run.reportId || state.finalizingRunId === state.run.id) {
    return;
  }
  state.finalizingRunId = state.run.id;
  try {
    state.latestReport = await api("/api/report/finalize", {
      method: "POST",
      body: JSON.stringify({ runId: state.run.id }),
    });
    if (state.run) {
      state.run.reportId = state.latestReport.reportId;
    }
    await refreshBootstrap();
  } catch (error) {
    console.warn("silent finalize failed", error);
  } finally {
    state.finalizingRunId = "";
  }
}

function finalizeRunWithBeacon() {
  if (!navigator.sendBeacon || !state.run?.id || Number(state.run.answeredCount || 0) === 0 || state.run.reportId) {
    return;
  }
  const payload = JSON.stringify({ runId: state.run.id });
  navigator.sendBeacon("/api/report/finalize", new Blob([payload], { type: "application/json" }));
}

function buildAnswerReveal(correctAnswer, submittedAnswer) {
  const correct = new Set();
  const submitted = new Set();
  if (correctAnswer?.label) correct.add(correctAnswer.label);
  (correctAnswer?.keys || []).forEach((key) => correct.add(key));
  if (submittedAnswer?.label) submitted.add(submittedAnswer.label);
  (submittedAnswer?.keys || []).forEach((key) => submitted.add(key));
  return { correct, submitted };
}

function renderLeaderboard(payload) {
  const entries = Array.isArray(payload?.entries) ? payload.entries : [];
  if (!entries.length) {
    els.leaderboardRoot.innerHTML = `<div class="leaderboard-empty">暫無可顯示榜單。</div>`;
    return;
  }

  const maxScore = Math.max(...entries.map((entry) => Number(entry.score || 0)), 1);
  const podium = entries.slice(0, 3);
  const rest = entries.slice(3, 8);

  const podiumHtml = podium
    .map(
      (entry) => `
        <div class="podium-card rank-${entry.rank}">
          <span class="podium-rank">#${entry.rank}</span>
          <strong class="podium-name">${escapeHtml(entry.displayName || "")}</strong>
          <span class="podium-score">${Math.round(entry.score || 0)} 分</span>
        </div>
      `
    )
    .join("");

  const listHtml = rest
    .map((entry) => {
      const width = clampPercent((Number(entry.score || 0) / maxScore) * 100);
      return `
        <div class="leaderboard-row">
          <span class="leaderboard-rank">#${entry.rank}</span>
          <div class="leaderboard-name">
            <strong>${escapeHtml(entry.displayName || "")}</strong>
            <div class="leaderboard-bar"><i style="width:${width}%"></i></div>
          </div>
          <span class="leaderboard-score">${Math.round(entry.score || 0)}</span>
        </div>
      `;
    })
    .join("");

  els.leaderboardRoot.innerHTML = `
    <div class="leaderboard-podium">${podiumHtml}</div>
    ${listHtml ? `<div class="leaderboard-list">${listHtml}</div>` : ""}
  `;
}

function renderBadges(badges) {
  els.badgeStrip.innerHTML = (badges || [])
    .slice(0, 4)
    .map((badge) => `<span class="badge-chip">${escapeHtml(badge.title || "")}</span>`)
    .join("");
}

function syncModeButtons() {
  document.querySelectorAll("[data-mode]").forEach((button) => {
    button.classList.toggle("active", button.dataset.mode === state.mode);
  });
}

function syncEntryButtons() {
  document.querySelectorAll("[data-kind]").forEach((button) => {
    button.classList.toggle("active", button.dataset.kind === state.kind);
    button.disabled = !hasAvailableChallenges(button.dataset.kind || "");
  });
}

function hasAvailableChallenges(kind) {
  const stats = state.bootstrap?.stats || {};
  if (kind === "function_word") {
    return Number(stats.functionChallenges || 0) > 0;
  }
  if (kind === "content_word") {
    return Number(stats.contentChallenges || 0) > 0;
  }
  return false;
}

function setTheme(theme) {
  const normalized = THEMES.includes(theme) ? theme : "parchment";
  state.theme = normalized;
  applyTheme(normalized);
  window.localStorage.setItem(THEME_KEY, normalized);
}

function applyTheme(theme) {
  document.documentElement.dataset.theme = theme;
  document.querySelectorAll("[data-theme]").forEach((button) => {
    button.classList.toggle("active", button.dataset.theme === theme);
  });
}

function loadTheme() {
  const stored = window.localStorage.getItem(THEME_KEY);
  return THEMES.includes(stored) ? stored : "parchment";
}

function deriveRank(score, streak, accuracy) {
  if (score >= 120 || accuracy >= 92) return "壓卷";
  if (score >= 80 || streak >= 5) return "爭衡";
  if (score >= 45 || accuracy >= 70) return "入勢";
  if (score >= 18 || streak >= 2) return "破題";
  return "初鋒";
}

function questionTypeLabel(questionType) {
  return {
    xuci_pair_compare: "辨義辨用",
    content_gloss: "定義",
    translation_keypoint: "翻譯",
    sentence_meaning: "句意",
    passage_meaning: "文意",
    analysis_short: "要點",
  }[questionType] || questionType;
}

function kindLabel(kind) {
  return kind === "function_word" ? "虛詞" : "實詞";
}

function modeLabel(mode) {
  return {
    warmup: "熱身",
    ranked: "排位",
    review: "追擊",
  }[mode] || mode;
}

function termHeadword(termId) {
  return String(termId || "").split("::").pop() || "";
}

function highlightTerm(text, headword) {
  const safe = escapeHtml(text || "");
  if (!headword) return safe;
  const escapedHeadword = escapeRegex(escapeHtml(headword));
  return safe.replace(new RegExp(escapedHeadword, "g"), `<span class="term-hit">${escapeHtml(headword)}</span>`);
}

function clampPercent(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return 0;
  return Math.max(0, Math.min(100, Math.round(numeric)));
}

function mergeBadges(existing, incoming) {
  const merged = [...(existing || [])];
  const seen = new Set(merged.map((badge) => badge?.key || badge?.title || ""));
  (incoming || []).forEach((badge) => {
    const key = badge?.key || badge?.title || "";
    if (!key || seen.has(key)) return;
    seen.add(key);
    merged.push(badge);
  });
  return merged;
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

function escapeAttr(value) {
  return escapeHtml(value).replaceAll("'", "&#39;");
}

function escapeRegex(value) {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
