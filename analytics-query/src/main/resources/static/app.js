"use strict";

const API_URL = "/api/analytics/query";

const form = document.querySelector("#question-form");
const questionInput = document.querySelector("#question");
const characterCount = document.querySelector("#character-count");
const submitButton = document.querySelector("#submit-button");
const clarification = document.querySelector("#clarification");
const clarificationMessage = document.querySelector("#clarification-message");
const loading = document.querySelector("#loading");
const errorState = document.querySelector("#error-state");
const errorMessage = document.querySelector("#error-message");
const results = document.querySelector("#results");
const resultCount = document.querySelector("#result-count");
const explanationCard = document.querySelector("#explanation-card");
const explanationText = document.querySelector("#explanation-text");
const emptyResults = document.querySelector("#empty-results");
const tableCard = document.querySelector("#table-card");
const resultsHead = document.querySelector("#results-head");
const resultsBody = document.querySelector("#results-body");

questionInput.addEventListener("input", () => {
  characterCount.textContent = `${questionInput.value.length.toLocaleString()} / 2,000`;
  hideClarification();
});

questionInput.addEventListener("keydown", (event) => {
  if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
    event.preventDefault();
    form.requestSubmit();
  }
});

document.querySelectorAll(".example-question").forEach((button) => {
  button.addEventListener("click", () => {
    questionInput.value = button.textContent.trim();
    questionInput.dispatchEvent(new Event("input"));
    questionInput.focus();
  });
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const question = questionInput.value.trim();

  if (!question) {
    showClarification("Please enter a question about network activity.");
    questionInput.focus();
    return;
  }

  startLoading();

  try {
    const response = await fetch(API_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question })
    });

    const payload = await readPayload(response);

    if (!response.ok) {
      throw new Error(payload.message || "The request could not be completed.");
    }

    handleResponse(payload);
  } catch (error) {
    showError(error instanceof Error ? error.message : "The service is temporarily unavailable.");
  } finally {
    stopLoading();
  }
});

async function readPayload(response) {
  const contentType = response.headers.get("content-type") || "";
  if (!contentType.includes("application/json")) {
    return {};
  }
  return response.json();
}

function handleResponse(payload) {
  if (payload.status === "SUCCESS") {
    renderResults(Array.isArray(payload.data) ? payload.data : [], payload.explanation);
    return;
  }

  if (payload.status === "NEEDS_CLARIFICATION") {
    showClarification(payload.message || "Please add a little more detail to your question.");
    questionInput.focus();
    return;
  }

  showError(friendlyMessage(payload.status, payload.message));
}

function renderResults(rows, explanation) {
  hideTransientStates();
  results.hidden = false;
  resultCount.textContent = `${rows.length.toLocaleString()} ${rows.length === 1 ? "result" : "results"}`;

  if (explanation) {
    explanationText.textContent = explanation;
    explanationCard.hidden = false;
  } else {
    explanationCard.hidden = true;
    explanationText.textContent = "";
  }

  if (rows.length === 0) {
    emptyResults.hidden = false;
    tableCard.hidden = true;
  } else {
    emptyResults.hidden = true;
    tableCard.hidden = false;
    renderTable(rows);
  }

  results.scrollIntoView({ behavior: "smooth", block: "start" });
}

function renderTable(rows) {
  const columns = collectColumns(rows);
  resultsHead.replaceChildren();
  resultsBody.replaceChildren();

  const headerRow = document.createElement("tr");
  columns.forEach((column) => {
    const heading = document.createElement("th");
    heading.scope = "col";
    heading.textContent = humanize(column);
    headerRow.appendChild(heading);
  });
  resultsHead.appendChild(headerRow);

  rows.forEach((row) => {
    const tableRow = document.createElement("tr");
    columns.forEach((column) => {
      const cell = document.createElement("td");
      cell.textContent = formatValue(row[column]);
      tableRow.appendChild(cell);
    });
    resultsBody.appendChild(tableRow);
  });
}

function collectColumns(rows) {
  const columns = [];
  const seen = new Set();
  rows.forEach((row) => {
    Object.keys(row || {}).forEach((column) => {
      if (!seen.has(column)) {
        seen.add(column);
        columns.push(column);
      }
    });
  });
  return columns;
}

function humanize(value) {
  return value
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/_/g, " ")
    .replace(/\bip\b/gi, "IP")
    .replace(/\bl7\b/gi, "L7")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function formatValue(value) {
  if (value === null || value === undefined || value === "") {
    return "—";
  }
  if (typeof value === "number") {
    return value.toLocaleString();
  }
  if (typeof value === "boolean") {
    return value ? "Yes" : "No";
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

function friendlyMessage(status, message) {
  if (message) {
    return message;
  }

  const messages = {
    OUT_OF_SCOPE: "Ask about network traffic, connections, bandwidth, IP addresses, or protocols.",
    UNSUPPORTED_ANALYSIS: "That insight is not available from the current network records. Try asking about observed activity instead.",
    REJECTED: "This request cannot be processed. Please ask a read-only question about network activity.",
    QUERY_REJECTED: "That question could not be answered safely. Try simplifying or narrowing the request.",
    DEPENDENCY_UNAVAILABLE: "The analytics service is temporarily unavailable. Please try again shortly.",
    EXECUTION_FAILED: "The analysis could not be completed. Please revise the question or try again."
  };

  return messages[status] || "The request could not be completed. Please revise your question and try again.";
}

function startLoading() {
  hideTransientStates();
  results.hidden = true;
  loading.hidden = false;
  submitButton.disabled = true;
  submitButton.querySelector("span").textContent = "Exploring…";
}

function stopLoading() {
  loading.hidden = true;
  submitButton.disabled = false;
  submitButton.querySelector("span").textContent = "Explore data";
}

function showClarification(message) {
  hideTransientStates();
  results.hidden = true;
  clarificationMessage.textContent = message;
  clarification.hidden = false;
}

function hideClarification() {
  clarification.hidden = true;
  clarificationMessage.textContent = "";
}

function showError(message) {
  hideTransientStates();
  results.hidden = true;
  errorMessage.textContent = message;
  errorState.hidden = false;
  errorState.scrollIntoView({ behavior: "smooth", block: "center" });
}

function hideTransientStates() {
  hideClarification();
  errorState.hidden = true;
  errorMessage.textContent = "";
}
