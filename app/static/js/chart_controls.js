// app/static/js/chart_controls.js
const el = document.getElementById("attendanceChart");
if (el && typeof Chart !== "undefined") {
  const ctx = el.getContext("2d");
  let smoothness = 3;

  function movingAverage(data, windowSize) {
    if (!Array.isArray(data)) return [];
    const out = [];
    for (let i = 0; i < data.length; i++) {
      const start = Math.max(0, i - windowSize + 1);
      const subset = data.slice(start, i + 1);
      const sum = subset.reduce((a, b) => a + (Number(b) || 0), 0);
      out.push(subset.length ? sum / subset.length : 0);
    }
    return out;
  }

  function buildChart() {
    const datasetPresent = movingAverage(window.presents || [], smoothness);
    const datasetFTR     = movingAverage(window.ftrs || [], smoothness);
    const datasetExcused = movingAverage(window.excused || [], smoothness);

    return new Chart(ctx, {
      type: "line",
      data: {
        labels: window.labels || [],
        datasets: [
          { label: "Present", data: datasetPresent, borderColor: "#00b050", fill: false },
          { label: "FTR",     data: datasetFTR,     borderColor: "#c00000", fill: false },
          { label: "Excused", data: datasetExcused, borderColor: "#ffc000", fill: false }
        ]
      },
      options: { responsive: true, animation: false }
    });
  }

  let chart = buildChart();
  const slider = document.getElementById("smoothness");
  if (slider) {
    slider.addEventListener("input", (e) => {
      smoothness = parseInt(e.target.value || "3", 10);
      chart.destroy();
      chart = buildChart();
    });
  }
}
