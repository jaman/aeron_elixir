(function () {
  var key = "aeron-elixir-theme";
  function apply(choice) {
    var root = document.documentElement;
    if (choice === "light" || choice === "dark") { root.setAttribute("data-theme", choice); }
    else { root.removeAttribute("data-theme"); }
  }
  window.aeronSetTheme = function (choice) {
    try { window.localStorage.setItem(key, choice); } catch (error) {}
    apply(choice);
  };
  var stored = "auto";
  try { stored = window.localStorage.getItem(key) || "auto"; } catch (error) {}
  apply(stored);
})();
