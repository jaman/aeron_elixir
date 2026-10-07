(function () {
  var reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  var bar = document.querySelector(".nav nav");
  if (bar) { followSections(bar); }
  themeMenu(document.querySelector("[data-theme-menu]"));

  function followSections(bar) {
    var entries = Array.prototype.slice.call(bar.querySelectorAll("a[data-section]"))
      .map(function (link) { return { link: link, section: document.getElementById(link.getAttribute("data-section")) }; })
      .filter(function (entry) { return entry.section; });
    if (entries.length === 0) { return; }

    var marker = document.createElement("span");
    marker.className = "nav-marker";
    marker.setAttribute("aria-hidden", "true");
    bar.insertBefore(marker, bar.firstChild);

    var header = document.querySelector(".nav");
    var current = null;
    var scheduled = false;

    function sectionInView() {
      var root = document.documentElement;
      if (window.innerHeight + window.scrollY >= root.scrollHeight - 2) { return entries[entries.length - 1]; }
      var line = header.offsetHeight + Math.round(window.innerHeight * 0.25);
      return entries.reduce(function (found, entry) {
        return entry.section.getBoundingClientRect().top <= line ? entry : found;
      }, null);
    }

    function placeMarker() {
      bar.classList.toggle("has-marker", current !== null);
      if (current === null) { return; }
      marker.style.width = current.link.offsetWidth + "px";
      marker.style.transform = "translateX(" + current.link.offsetLeft + "px)";
    }

    function keepVisible(link) {
      var left = link.offsetLeft;
      var right = left + link.offsetWidth;
      if (left < bar.scrollLeft || right > bar.scrollLeft + bar.clientWidth) {
        bar.scrollTo({ left: Math.max(0, left - 16), behavior: reducedMotion ? "auto" : "smooth" });
      }
    }

    function update() {
      scheduled = false;
      var next = sectionInView();
      if (next !== current) {
        if (current) { current.link.removeAttribute("aria-current"); }
        if (next) { next.link.setAttribute("aria-current", "location"); keepVisible(next.link); }
        current = next;
      }
      placeMarker();
    }

    function schedule() {
      if (!scheduled) { scheduled = true; window.requestAnimationFrame(update); }
    }

    window.addEventListener("scroll", schedule, { passive: true });
    window.addEventListener("resize", schedule);
    if (document.fonts && document.fonts.ready) { document.fonts.ready.then(schedule); }
    update();
    window.requestAnimationFrame(function () { bar.classList.add("marker-ready"); });
  }

  function themeMenu(menu) {
    if (!menu) { return; }
    var toggle = menu.querySelector(".theme-toggle");
    var list = menu.querySelector(".theme-options");
    var items = Array.prototype.slice.call(list.querySelectorAll("[data-theme-choice]"));

    function chosen() { return document.documentElement.getAttribute("data-theme") || "auto"; }

    function markChosen() {
      var choice = chosen();
      items.forEach(function (item) {
        item.setAttribute("aria-checked", String(item.getAttribute("data-theme-choice") === choice));
      });
    }

    function open() {
      markChosen();
      list.hidden = false;
      toggle.setAttribute("aria-expanded", "true");
      (items.find(function (item) { return item.getAttribute("aria-checked") === "true"; }) || items[0]).focus({ preventScroll: true });
    }

    function close(returnFocus) {
      if (list.hidden) { return; }
      list.hidden = true;
      toggle.setAttribute("aria-expanded", "false");
      if (returnFocus) { toggle.focus({ preventScroll: true }); }
    }

    function moveFocus(step) {
      var index = items.indexOf(document.activeElement);
      items[(index + step + items.length) % items.length].focus({ preventScroll: true });
    }

    toggle.addEventListener("click", function () { if (list.hidden) { open(); } else { close(false); } });

    items.forEach(function (item) {
      item.addEventListener("click", function () {
        window.aeronSetTheme(item.getAttribute("data-theme-choice"));
        markChosen();
        close(true);
      });
    });

    list.addEventListener("keydown", function (event) {
      var keys = {
        ArrowDown: function () { moveFocus(1); },
        ArrowUp: function () { moveFocus(-1); },
        Home: function () { items[0].focus({ preventScroll: true }); },
        End: function () { items[items.length - 1].focus({ preventScroll: true }); },
        Escape: function () { close(true); },
        Tab: function () { close(false); }
      };
      if (!keys[event.key]) { return; }
      if (event.key !== "Tab") { event.preventDefault(); }
      keys[event.key]();
    });

    document.addEventListener("click", function (event) {
      if (!menu.contains(event.target)) { close(false); }
    });

    markChosen();
  }
})();
