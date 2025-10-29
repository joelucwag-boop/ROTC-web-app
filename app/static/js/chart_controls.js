// Simple Chart.js plot with smoothing slider
const ctx = document.getElementById("attendanceChart").getContext("2d");
let smoothness = 3;

function movingAverage(data, windowSize) {
  const avg = [];
  for (let i = 0; i < data.length; i++) {
    const start = Math.max(0, i - windowSize + 1);
    const subset = data.slice(start, i + 1);
    const sum = subset.reduce((a, b) => a + b, 0);
    avg.push(sum / subset.length);
  }
  return avg;
}

function buildChart() {
  const chartData = {
    labels: labels,
    datasets: [
      { label: "Present", data: movingAverage(presents, smoothness), borderColor: "#00b050" },
      { label: "FTR", data: movingAverage(ftrs, smoothness), borderColor: "#c00000" },
      { label: "Excused", data: movingAverage(excused, smoothness), borderColor: "#ffc000" }
    ]
  };
  return new Chart(ctx, { type: "line", data: chartData, options: { responsive: true } });
}

let chart = buildChart();

document.getElementById("smoothness").addEventListener("input", e => {
  smoothness = parseInt(e.target.value);
  chart.destroy();
  chart = buildChart();
});
