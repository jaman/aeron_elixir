(function () {
  var explorer = document.querySelector("[data-explorer]");
  if (!explorer) { return; }
  var tabs = explorer.querySelectorAll("[data-tab]");
  var panels = explorer.querySelectorAll("[data-tab-panel]");

  function showTab(name) {
    tabs.forEach(function (tab) { tab.setAttribute("aria-selected", String(tab.dataset.tab === name)); });
    panels.forEach(function (panel) { panel.hidden = panel.dataset.tabPanel !== name; });
  }

  function selectionKey(panel) {
    return Array.prototype.map.call(panel.querySelectorAll("[data-field]"), function (control) {
      var pressed = control.querySelector("[aria-pressed=\"true\"]");
      return control.dataset.field + "=" + pressed.dataset.option;
    }).join("&");
  }

  function showChart(panel) {
    var key = selectionKey(panel);
    panel.querySelectorAll("[data-chart]").forEach(function (chart) { chart.hidden = chart.dataset.chart !== key; });
  }

  function press(control, button) {
    control.querySelectorAll("[data-option]").forEach(function (option) {
      option.setAttribute("aria-pressed", String(option === button));
    });
  }

  tabs.forEach(function (tab) {
    tab.addEventListener("click", function () { showTab(tab.dataset.tab); });
  });

  panels.forEach(function (panel) {
    panel.querySelectorAll("[data-field]").forEach(function (control) {
      control.querySelectorAll("[data-option]").forEach(function (button) {
        button.addEventListener("click", function () {
          press(control, button);
          showChart(panel);
        });
      });
    });
  });

  explorer.querySelectorAll("[data-toggle-table]").forEach(function (button) {
    button.addEventListener("click", function () { explorer.classList.toggle("show-table"); });
  });
})();
