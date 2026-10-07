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

  compareClients(explorer.querySelector("[data-compare]"));

  function compareClients(compare) {
    if (!compare) { return; }
    var storageKey = "aeron-elixir-compare";
    var toggle = compare.querySelector("[data-compare-toggle]");
    var list = compare.querySelector("[data-compare-options]");
    var boxes = Array.prototype.slice.call(list.querySelectorAll("input[type=checkbox]"));
    var chips = Array.prototype.slice.call(compare.querySelectorAll("[data-chip]"));

    function chosen() {
      return boxes.filter(function (box) { return box.checked; }).map(function (box) { return box.value; });
    }

    function shown(element, selected) {
      return element.dataset.family === "elixir" || selected.indexOf(element.dataset.client) !== -1;
    }

    function rescale(chart) {
      var rows = Array.prototype.slice.call(chart.querySelectorAll(".bar-row:not([hidden])"));
      var largest = rows.reduce(function (most, row) { return Math.max(most, Number(row.dataset.value)); }, 0) || 1;
      rows.forEach(function (row) {
        row.querySelector(".bar").style.width = "calc((100% - 6.5rem) * " + Number(row.dataset.value) / largest + ")";
      });
      chart.querySelectorAll("[data-legend-family]").forEach(function (item) {
        item.hidden = !chart.querySelector(".bar-row.fam-" + item.dataset.legendFamily + ":not([hidden])");
      });
    }

    function apply(selected) {
      boxes.forEach(function (box) { box.checked = selected.indexOf(box.value) !== -1; });
      chips.forEach(function (chip) { chip.hidden = selected.indexOf(chip.dataset.chip) === -1; });
      explorer.querySelectorAll("[data-client]").forEach(function (element) { element.hidden = !shown(element, selected); });
      explorer.querySelectorAll("[data-chart]").forEach(rescale);
      try { window.localStorage.setItem(storageKey, JSON.stringify(selected)); } catch (error) {}
    }

    function stored() {
      try {
        var saved = JSON.parse(window.localStorage.getItem(storageKey));
        return Array.isArray(saved) ? saved : null;
      } catch (error) { return null; }
    }

    function open() {
      list.hidden = false;
      toggle.setAttribute("aria-expanded", "true");
      boxes[0].focus({ preventScroll: true });
    }

    function close(returnFocus) {
      if (list.hidden) { return; }
      list.hidden = true;
      toggle.setAttribute("aria-expanded", "false");
      if (returnFocus) { toggle.focus({ preventScroll: true }); }
    }

    toggle.addEventListener("click", function () { if (list.hidden) { open(); } else { close(false); } });
    boxes.forEach(function (box) { box.addEventListener("change", function () { apply(chosen()); }); });
    chips.forEach(function (chip) {
      chip.addEventListener("click", function () {
        apply(chosen().filter(function (key) { return key !== chip.dataset.chip; }));
      });
    });
    list.querySelectorAll("[data-compare-all]").forEach(function (button) {
      button.addEventListener("click", function () {
        apply(button.dataset.compareAll === "all" ? boxes.map(function (box) { return box.value; }) : []);
      });
    });
    list.addEventListener("keydown", function (event) { if (event.key === "Escape") { event.preventDefault(); close(true); } });
    document.addEventListener("click", function (event) { if (!compare.contains(event.target)) { close(false); } });

    apply(stored() || chosen());
  }
})();
