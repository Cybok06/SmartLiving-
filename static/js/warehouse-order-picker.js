(() => {
  const API_SEARCH = "/manager/orders/products";
  const API_PREFETCH = "/manager/orders/products_prefetch";
  const PREFETCH_LIMIT = 300;
  const DISPLAY_LIMIT = 30;

  let prefetchCache = [];
  let prefetchPromise = null;

  const formatMetaText = (product) => {
    if (!product) {
      return "";
    }
    const parts = [];
    if (product.qty_available !== undefined && product.qty_available !== null) {
      parts.push(`Available: ${product.qty_available}`);
    }
    if (product.tag) {
      parts.push(product.tag);
    }
    if (product.sku) {
      parts.push(`SKU: ${product.sku}`);
    }
    return parts.join(" · ");
  };

  const createMessageItem = (text) => {
    const div = document.createElement("div");
    div.className = "list-group-item small text-muted";
    div.textContent = text || "No matching products";
    return div;
  };

  const createSuggestionItem = (product, onSelect) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "list-group-item list-group-item-action d-flex align-items-start gap-2 py-2";
    const img = product.image_url
      ? `<img src="${product.image_url}" alt="${product.name || "product"}" class="suggest-img rounded">`
      : "";
    button.innerHTML = `
      ${img}
      <div class="flex-grow-1 text-start">
        <div class="fw-semibold small mb-1">${product.name || "Unnamed product"}</div>
        <div class="small text-muted">${formatMetaText(product)}</div>
      </div>
    `;
    button.addEventListener("click", (ev) => {
      ev.preventDefault();
      onSelect(product);
    });
    return button;
  };

  const renderMessage = (box, message) => {
    box.innerHTML = "";
    box.appendChild(createMessageItem(message));
    box.classList.remove("d-none");
  };

  const renderSuggestions = (items, box, options = {}, row) => {
    box.innerHTML = "";
    const limit = options.limit || DISPLAY_LIMIT;
    const subset = (items || []).slice(0, limit);
    if (!subset.length) {
      renderMessage(box, options.emptyMessage || "No products found.");
      return;
    }
    subset.forEach((product) => {
      const item = createSuggestionItem(product, (selected) => {
        selectProduct(row, selected);
        box.classList.add("d-none");
      });
      box.appendChild(item);
    });
    box.classList.remove("d-none");
  };

  const loadPrefetch = async () => {
    if (prefetchPromise) {
      return prefetchPromise;
    }
    prefetchPromise = (async () => {
      try {
        const url = `${API_PREFETCH}?limit=${PREFETCH_LIMIT}`;
        console.debug("[WarehouseOrderPicker] prefetch", url);
        const res = await fetch(url);
        if (!res.ok) {
          throw new Error(`Prefetch failed (${res.status})`);
        }
        const json = await res.json();
        prefetchCache = (json?.results || []).slice();
        console.debug("[WarehouseOrderPicker] prefetch count", prefetchCache.length);
      } catch (err) {
        console.error("[WarehouseOrderPicker] prefetch error", err);
        prefetchCache = [];
      }
    })();
    return prefetchPromise;
  };

  const filterLocalProducts = (query) => {
    if (!prefetchCache.length) {
      return [];
    }
    if (!query) {
      return prefetchCache.slice(0, DISPLAY_LIMIT);
    }
    const matches = [];
    const needle = query.toLowerCase();
    for (const product of prefetchCache) {
      const combined = [
        product.name || "",
        product.sku || "",
        product.tag || "",
      ]
        .map((v) => v.toLowerCase())
        .join(" ");
      if (combined.includes(needle)) {
        matches.push(product);
        if (matches.length >= DISPLAY_LIMIT) {
          break;
        }
      }
    }
    return matches;
  };

  const selectProduct = (row, product) => {
    const input = row.querySelector(".product-search");
    const hidden = row.querySelector(".product-id");
    const meta = row.querySelector(".product-meta");
    const qtyInput = row.querySelector(".qty");
    const availability = row.querySelector(".availability-msg");
    hidden.value = product._id || "";
    input.value = product.name || "";
    meta.textContent = formatMetaText(product);
    row.dataset.productTag = product.tag || "Warehouse";
    const maxQty = product.qty_available || 0;
    if (maxQty) {
      qtyInput.dataset.maxQty = maxQty;
      qtyInput.setAttribute("max", maxQty);
      if (!qtyInput.value || parseInt(qtyInput.value, 10) < 1) {
        qtyInput.value = 1;
      }
    } else {
      qtyInput.dataset.maxQty = "";
      qtyInput.removeAttribute("max");
    }
    availability.classList.add("d-none");
  };

  const updateAvailabilityMessage = (row) => {
    const qtyInput = row.querySelector(".qty");
    const availability = row.querySelector(".availability-msg");
    const maxQty = parseInt(qtyInput.dataset.maxQty || "0", 10) || 0;
    const value = parseInt(qtyInput.value || "0", 10) || 0;
    if (maxQty && value > maxQty) {
      const tag = row.dataset.productTag || "this product";
      availability.textContent = `Only ${maxQty} units available from ${tag}.`;
      availability.classList.remove("d-none");
      return;
    }
    availability.classList.add("d-none");
  };

  const fetchRemoteProducts = async (query, box, row, requestId) => {
    if (query.length < 2) {
      return;
    }
    const url = `${API_SEARCH}?q=${encodeURIComponent(query)}`;
    console.debug("[WarehouseOrderPicker] fetching", url);
    try {
      const res = await fetch(url);
      if (requestId !== row.dataset.remoteRequestId) {
        return;
      }
      if (!res.ok) {
        renderMessage(box, `Search failed (${res.status})`);
        return;
      }
      const json = await res.json();
      const results = json.results || [];
      console.debug("[WarehouseOrderPicker] results", results.length);
      if (requestId !== row.dataset.remoteRequestId) {
        return;
      }
      if (!results.length) {
        renderMessage(box, "No products found");
        return;
      }
      renderSuggestions(results, box, { limit: DISPLAY_LIMIT }, row);
    } catch (err) {
      if (requestId !== row.dataset.remoteRequestId) {
        return;
      }
      console.error("[WarehouseOrderPicker] search error", err);
      renderMessage(box, "Search failed");
    }
  };

  const attachRowListeners = (row) => {
    const searchInput = row.querySelector(".product-search");
    const hiddenInput = row.querySelector(".product-id");
    const suggestBox = row.querySelector(".suggest-box");
    const qtyInput = row.querySelector(".qty");
    const removeBtn = row.querySelector(".remove-row");
    let hideTimer = null;
    let localTimer = null;

    const openPrefetch = () => {
      if (prefetchCache.length) {
        renderSuggestions(prefetchCache.slice(0, DISPLAY_LIMIT), suggestBox, { emptyMessage: "No products loaded yet." }, row);
        return;
      }
      renderMessage(suggestBox, "Loading warehouse products...");
      loadPrefetch().then(() => {
        if (document.activeElement === searchInput) {
          renderSuggestions(prefetchCache.slice(0, DISPLAY_LIMIT), suggestBox, { emptyMessage: "No products loaded yet." }, row);
        }
      });
    };

    const handleSearchInput = () => {
      const query = searchInput.value.trim();
      hiddenInput.value = "";
      qtyInput.dataset.maxQty = "";
      qtyInput.removeAttribute("max");
      if (localTimer) {
        clearTimeout(localTimer);
      }
      const matches = filterLocalProducts(query);
      renderSuggestions(matches, suggestBox, { emptyMessage: query ? "No local matches" : "No products yet." }, row);
      if (query.length >= 2) {
        const requestId = String(Date.now());
        row.dataset.remoteRequestId = requestId;
        localTimer = setTimeout(() => {
          fetchRemoteProducts(query, suggestBox, row, requestId);
        }, 220);
      }
    };

    searchInput.addEventListener("focus", () => {
      if (hideTimer) {
        clearTimeout(hideTimer);
      }
      openPrefetch();
    });

    searchInput.addEventListener("input", () => {
      handleSearchInput();
    });

    searchInput.addEventListener("blur", () => {
      hideTimer = setTimeout(() => {
        suggestBox.classList.add("d-none");
      }, 150);
    });

    qtyInput.addEventListener("input", () => {
      updateAvailabilityMessage(row);
    });

    removeBtn.addEventListener("click", (event) => {
      event.preventDefault();
      row.remove();
    });
  };

  const createRow = (defaults = {}) => {
    const tr = document.createElement("tr");
    tr.className = "warehouse-order-row";
    tr.innerHTML = `
      <td class="position-relative">
        <input type="text" class="form-control product-search" placeholder="Search warehouse products" autocomplete="off">
        <input type="hidden" class="product-id">
        <div class="form-text product-meta text-muted small mt-1"></div>
        <div class="list-group suggest-box d-none"></div>
      </td>
      <td>
        <input type="number" min="1" class="form-control qty" value="1">
        <div class="form-text availability-msg text-danger small mt-1 d-none"></div>
      </td>
      <td><input type="date" class="form-control expected-date"></td>
      <td><input class="form-control notes"></td>
      <td><button type="button" class="btn btn-sm btn-link text-danger remove-row" title="Remove row">&times;</button></td>
    `;

    attachRowListeners(tr);

    if (defaults.product) {
      selectProduct(tr, defaults.product);
    }
    if (typeof defaults.qty !== "undefined") {
      const qtyInput = tr.querySelector(".qty");
      qtyInput.value = defaults.qty;
    }
    if (defaults.expected_date) {
      tr.querySelector(".expected-date").value = defaults.expected_date;
    }
    if (defaults.notes) {
      tr.querySelector(".notes").value = defaults.notes;
    }

    return tr;
  };

  const addRow = (container, defaults) => {
    const tr = createRow(defaults);
    container.appendChild(tr);
    return tr;
  };

  const gatherItems = (container) => {
    const rows = Array.from(container.querySelectorAll("tr"));
    const items = [];
    const errors = [];

    rows.forEach((row, index) => {
      const productId = row.querySelector(".product-id").value.trim();
      const qtyInput = row.querySelector(".qty");
      const expected = row.querySelector(".expected-date").value || null;
      const notes = row.querySelector(".notes").value.trim();
      const qty = parseInt(qtyInput.value || "0", 10) || 0;
      const maxQty = parseInt(qtyInput.dataset.maxQty || "0", 10) || 0;

      if (!productId) {
        if (qty > 0 || row.querySelector(".product-search").value.trim()) {
          errors.push(`Row ${index + 1}: select a warehouse product.`);
        }
        return;
      }

      if (qty <= 0) {
        errors.push(`Row ${index + 1}: quantity must be at least 1.`);
        return;
      }

      if (maxQty && qty > maxQty) {
        errors.push(
          `Row ${index + 1}: requested quantity (${qty}) exceeds available stock (${maxQty}).`
        );
        return;
      }

      items.push({
        product_id: productId,
        qty,
        expected_date: expected,
        notes,
      });
    });

    return { items, errors };
  };

  window.WarehouseOrderPicker = {
    addRow,
    gatherItems,
  };

  loadPrefetch();
})();
