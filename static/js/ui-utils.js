(function () {
  function toNumber(value) {
    if (value === null || value === undefined || value === "") return 0;
    if (typeof value === "number") return value;
    var cleaned = String(value).replace(/,/g, "").trim();
    var num = Number(cleaned);
    return Number.isFinite(num) ? num : 0;
  }

  function formatNumber(value, decimals) {
    var d = typeof decimals === "number" ? decimals : 0;
    var num = toNumber(value);
    try {
      return new Intl.NumberFormat("en-US", {
        minimumFractionDigits: d,
        maximumFractionDigits: d,
      }).format(num);
    } catch (e) {
      return d ? num.toFixed(d) : String(Math.round(num));
    }
  }

  function formatMoney(value) {
    return formatNumber(value, 2);
  }

  function formatAllNumbers(root) {
    var scope = root || document;
    var nodes = scope.querySelectorAll("[data-money],[data-number],.js-money,.js-number");
    nodes.forEach(function (el) {
      var raw = el.getAttribute("data-value");
      var decimals = el.getAttribute("data-decimals");
      var isMoney = el.hasAttribute("data-money") || el.classList.contains("js-money");
      var isNumber = el.hasAttribute("data-number") || el.classList.contains("js-number");
      var d = decimals !== null ? parseInt(decimals, 10) : (isMoney ? 2 : 0);
      var value = raw !== null ? raw : el.textContent;
      if (!isMoney && !isNumber && d === 0) return;
      el.textContent = formatNumber(value, d);
    });
  }

  window.formatNumber = formatNumber;
  window.formatMoney = formatMoney;
  window.formatAllNumbers = formatAllNumbers;

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () {
      formatAllNumbers();
    });
  } else {
    formatAllNumbers();
  }
})();
