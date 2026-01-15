(function () {
  const inventoryGrid = document.getElementById('inventoryGrid');
  const modalsContainer = document.getElementById('inventoryModals');
  const filterForm = document.getElementById('filterForm');
  const resetBtn = document.getElementById('resetFiltersBtn');
  const loadMoreWrap = document.getElementById('loadMoreWrap');
  const loadMoreBtn = document.getElementById('loadMoreBtn');
  const loadMoreMeta = document.getElementById('loadMoreMeta');
  const toastContainer = document.getElementById('toastContainer');
  if (!inventoryGrid) return;

  let parsedBranches = [];
  try {
    parsedBranches = JSON.parse(inventoryGrid.dataset.branches || '[]');
  } catch (err) {
    parsedBranches = [];
  }
  const BRANCHES = Array.isArray(parsedBranches) ? parsedBranches : [];
  const DEFAULT_LIMIT = parseInt(inventoryGrid.dataset.defaultLimit || '50', 10) || 50;
  const LIST_ENDPOINT = inventoryGrid.dataset.listEndpoint || '/inventory_products/api/list';
  const UPDATE_ENDPOINT = inventoryGrid.dataset.updateEndpoint || '/inventory_products/api/update';
  const DELETE_ENDPOINT = inventoryGrid.dataset.deleteEndpoint || '/inventory_products/api/delete';
  const TRANSFER_ENDPOINT = inventoryGrid.dataset.transferEndpoint || '/inventory_products/api/transfer';
  const CHANGE_IMAGE_BASE = inventoryGrid.dataset.changeImageBase || '/inventory_products/api/change-image';

  const state = {
    manager: '',
    branch: '',
    product: '',
    low_stock: false,
    limit: DEFAULT_LIMIT,
    offset: 0,
    total_count: parseInt(inventoryGrid.dataset.totalCount || '0', 10) || 0,
    has_more: false,
    reorder_level: parseInt(inventoryGrid.dataset.reorderLevel || '0', 10) || 0,
    group_products: false
  };

  const escapeSelectorFn = window.escapeSelector || function (value) {
    const str = String(value ?? '');
    if (window.CSS && CSS.escape) {
      return CSS.escape(str);
    }
    return str.replace(/"/g, '\\"');
  };

  const escapeHtml = (value) => String(value ?? '').replace(/[&<>"']/g, (char) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;'
  })[char]);

  const safeDomId = (value) => String(value ?? '').replace(/[^a-zA-Z0-9_-]/g, '_');

  const formatMoney = (value, fallback = '-') => {
    if (value === null || value === undefined || value === '') return fallback;
    const num = Number(value);
    if (Number.isNaN(num)) return fallback;
    return num.toFixed(2);
  };

  const showToast = (type, message) => {
    if (!toastContainer || !window.bootstrap) return;
    const level = ['success', 'danger', 'warning', 'info'].includes(type) ? type : 'info';
    const toastEl = document.createElement('div');
    toastEl.className = `toast align-items-center text-bg-${level} border-0`;
    toastEl.setAttribute('role', 'alert');
    toastEl.setAttribute('aria-live', 'assertive');
    toastEl.setAttribute('aria-atomic', 'true');
    toastEl.innerHTML = `
      <div class="d-flex">
        <div class="toast-body">${escapeHtml(message || '')}</div>
        <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button>
      </div>
    `;
    toastContainer.appendChild(toastEl);
    const toast = bootstrap.Toast.getOrCreateInstance(toastEl, { delay: 3500 });
    toast.show();
    toastEl.addEventListener('hidden.bs.toast', () => toastEl.remove());
  };

  const setModalError = (form, message) => {
    const alertBox = form ? form.querySelector('.modal-error') : null;
    if (!alertBox) return;
    if (message) {
      alertBox.textContent = message;
      alertBox.classList.remove('d-none');
    } else {
      alertBox.textContent = '';
      alertBox.classList.add('d-none');
    }
  };

  const getFiltersFromForm = () => {
    if (!filterForm) return;
    const formData = new FormData(filterForm);
    state.manager = (formData.get('manager') || '').toString().trim();
    state.branch = (formData.get('branch') || '').toString().trim();
    state.product = (formData.get('product') || '').toString().trim();
    state.low_stock = !!formData.get('low_stock');
  };

  const applyFiltersToForm = () => {
    if (!filterForm) return;
    const managerSelect = filterForm.querySelector('[name="manager"]');
    const branchSelect = filterForm.querySelector('[name="branch"]');
    const productInput = filterForm.querySelector('[name="product"]');
    const lowStockInput = filterForm.querySelector('[name="low_stock"]');
    if (managerSelect) managerSelect.value = state.manager || '';
    if (branchSelect) branchSelect.value = state.branch || '';
    if (productInput) productInput.value = state.product || '';
    if (lowStockInput) lowStockInput.checked = !!state.low_stock;
  };

  const saveFilters = () => {
    const payload = {
      manager: state.manager,
      branch: state.branch,
      product: state.product,
      low_stock: state.low_stock
    };
    localStorage.setItem('inv_filters_v1', JSON.stringify(payload));
  };

  const updateUrlFromState = () => {
    const params = new URLSearchParams();
    if (state.manager) params.set('manager', state.manager);
    if (state.branch) params.set('branch', state.branch);
    if (state.product) params.set('product', state.product);
    if (state.low_stock) params.set('low_stock', '1');
    const newUrl = `${window.location.pathname}${params.toString() ? `?${params.toString()}` : ''}`;
    window.history.replaceState({}, '', newUrl);
  };

  const restoreFilters = () => {
    const params = new URLSearchParams(window.location.search);
    const hasUrlFilters = ['manager', 'branch', 'product', 'low_stock'].some((key) => params.has(key));
    if (hasUrlFilters) {
      state.manager = params.get('manager') || '';
      state.branch = params.get('branch') || '';
      state.product = params.get('product') || '';
      state.low_stock = ['1', 'true', 'yes', 'on'].includes((params.get('low_stock') || '').toLowerCase());
      saveFilters();
    } else {
      try {
        const stored = JSON.parse(localStorage.getItem('inv_filters_v1') || '{}');
        state.manager = stored.manager || '';
        state.branch = stored.branch || '';
        state.product = stored.product || '';
        state.low_stock = !!stored.low_stock;
      } catch (err) {
        state.manager = '';
        state.branch = '';
        state.product = '';
        state.low_stock = false;
      }
    }
    applyFiltersToForm();
    updateUrlFromState();
  };

  const resetFilters = () => {
    state.manager = '';
    state.branch = '';
    state.product = '';
    state.low_stock = false;
    localStorage.removeItem('inv_filters_v1');
    applyFiltersToForm();
    updateUrlFromState();
  };

  const renderSkeletons = (count = 6) => {
    inventoryGrid.innerHTML = Array.from({ length: count }).map(() => (
      '<div class="col"><div class="skeleton-card"></div></div>'
    )).join('');
  };

  const buildCardHtml = (item) => {
    const domId = safeDomId(item._id);
    const qty = Number(item.qty || 0);
    const legacyPrice = item.price;
    const sellingPrice = item.selling_price !== null && item.selling_price !== undefined
      ? Number(item.selling_price)
      : (item.price !== null && item.price !== undefined ? Number(item.price) : null);
    const costPrice = item.cost_price !== null && item.cost_price !== undefined ? Number(item.cost_price) : null;
    const calcMargin = (sellingPrice !== null && costPrice !== null) ? (sellingPrice - costPrice) : null;
    const pctMargin = (calcMargin !== null && costPrice) ? (calcMargin / costPrice * 100) : null;
    const isLowStock = !!item.is_low_stock;
    const grouped = !!item.grouped;
    const managerName = item.manager_name || '';
    const branchName = item.branch_name || '';
    const marginHtml = calcMargin === null
      ? '<span class="small-help">Margin pending (set S-Price &amp; C-Price)</span>'
      : (calcMargin > 0
        ? `<span class="badge-pos">Margin: GHS ${calcMargin.toFixed(2)}${pctMargin !== null ? ` (${pctMargin.toFixed(1)}%)` : ''}</span>`
        : (calcMargin < 0
          ? `<span class="badge-neg">Margin: GHS ${calcMargin.toFixed(2)}${pctMargin !== null ? ` (${pctMargin.toFixed(1)}%)` : ''}</span>`
          : '<span class="badge-zero">Margin: GHS 0.00</span>'));

    const managerHtml = managerName === 'Multiple'
      ? '<span class="text-info">Multiple managers</span>'
      : `${escapeHtml(managerName)} (${escapeHtml(branchName)})`;

    const lowStockHtml = isLowStock ? '<span class="low-stock-pill">LOW STOCK</span>' : '';

    return `
      <div class="col">
        <div class="card shadow-sm h-100 ${isLowStock ? 'low-stock low-stock-pulse' : ''}"
             id="inventoryCard${domId}"
             data-item-id="${escapeHtml(item._id)}"
             data-product-name="${escapeHtml(item.name)}"
             data-branch-name="${escapeHtml(branchName)}"
             data-manager-name="${escapeHtml(managerName)}"
             data-qty="${qty}"
             data-price="${escapeHtml(item.price ?? '')}"
             data-selling-price="${escapeHtml(item.selling_price ?? '')}"
             data-cost-price="${escapeHtml(item.cost_price ?? '')}"
             data-image-url="${escapeHtml(item.image_url || '')}"
             data-description="${escapeHtml(item.description || '')}"
             data-grouped="${grouped ? '1' : '0'}">
          <div class="position-relative">
            ${managerName === 'Multiple' ? '<span class="branch-pill manager-pill-multi">All branches (grouped)</span>' : `<span class="branch-pill">${escapeHtml(branchName)}</span>`}
            <img src="${escapeHtml(item.image_url || '')}" class="card-img-top img-cover product-toggle" alt="${escapeHtml(item.name)}">
            <button class="btn btn-sm btn-warning position-absolute top-0 end-0 m-2"
                    data-bs-toggle="modal"
                    data-bs-target="#changeImageModal${domId}"
                    title="Change image for this product">
              IMG
            </button>
          </div>
          <div class="card-body d-flex flex-column">
            <div class="d-flex justify-content-between align-items-start mb-1">
              <h5 class="card-title mb-1 product-toggle">${escapeHtml(item.name)}</h5>
              <button class="toggle-details"
                      type="button"
                      data-target="details${domId}"
                      aria-expanded="false"
                      aria-controls="details${domId}"
                      title="Show more">
                &gt;
              </button>
            </div>
            <div class="compact-meta">
              <button type="button"
                      class="product-chip qty-pill"
                      data-product-name="${escapeHtml(item.name)}"
                      data-product-id="${escapeHtml(item._id)}"
                      data-manager-name="${escapeHtml(managerName)}"
                      data-branch-name="${escapeHtml(branchName)}"
                      data-price="${escapeHtml(item.price ?? '')}"
                      data-selling-price="${escapeHtml(item.selling_price ?? '')}"
                      data-cost-price="${escapeHtml(item.cost_price ?? '')}"
                      data-description="${escapeHtml(item.description || '')}"
                      aria-label="View quantity distribution for ${escapeHtml(item.name)}">
                <span class="qty-icon">QTY</span>
                <span class="qty-text">QTYs</span>
                <span class="qty-badge">${qty}</span>
              </button>
              <span class="price-pill">GHS ${formatMoney(legacyPrice, '0.00')}</span>
              ${lowStockHtml}
            </div>

            <div class="product-details" id="details${domId}">
              <p class="card-text text-muted-quiet mb-2">${escapeHtml(item.description || '')}</p>

              <div class="badge bg-light text-dark border mb-2">
                Stock: <strong>${qty}</strong>
              </div>

              <div class="price-grid">
                <div>
                  <div class="price-label">Legacy Price</div>
                  <div class="price-chip"><span>GHS</span><strong>${formatMoney(legacyPrice, '0.00')}</strong></div>
                </div>
                <div>
                  <div class="price-label">Qty</div>
                  <div class="price-chip"><strong>${qty}</strong></div>
                </div>
                <div>
                  <div class="price-label">Selling (S-Price)</div>
                  <div class="price-chip">
                    <span>GHS</span>
                    <strong>${formatMoney(sellingPrice)}</strong>
                  </div>
                </div>
                <div>
                  <div class="price-label">Cost (C-Price)</div>
                  <div class="price-chip">
                    <span>GHS</span>
                    <strong>${formatMoney(costPrice)}</strong>
                  </div>
                </div>
              </div>

              <div class="mt-2">
                ${marginHtml}
              </div>

              <div class="divider"></div>

              <p class="card-text mb-2">
                <strong>Manager:</strong>
                ${managerHtml}
              </p>

              <div class="mt-auto d-flex flex-wrap gap-2">
                <button class="btn btn-sm btn-success"
                        data-bs-toggle="modal"
                        data-bs-target="#editModal${domId}">
                  Edit
                </button>
                ${managerName !== 'Multiple' ? `
                <button class="btn btn-sm btn-transfer"
                        data-bs-toggle="modal"
                        data-bs-target="#transferModal${domId}">
                  Transfer
                </button>` : ''}

                <form method="POST" class="d-inline flex-grow-1 delete-item-form" data-item="${escapeHtml(item._id)}">
                  <input type="hidden" name="item_id" value="${escapeHtml(item._id)}">
                  <div class="d-flex flex-wrap mb-2 mt-1">
                    <div class="form-check me-2">
                      <input class="form-check-input branch-master" type="checkbox" id="deleteAllBranches${domId}">
                      <label class="form-check-label small" for="deleteAllBranches${domId}">All</label>
                    </div>
                    ${BRANCHES.map((branch, idx) => `
                    <div class="form-check me-2">
                      <input class="form-check-input branch-option" type="checkbox" name="branches" value="${escapeHtml(branch)}" id="deleteBranch${domId}${idx}">
                      <label class="form-check-label small" for="deleteBranch${domId}${idx}">${escapeHtml(branch)}</label>
                    </div>`).join('')}
                  </div>
                  <button class="btn btn-sm btn-delete" name="action" value="delete">Delete</button>
                </form>

                <button class="btn btn-sm btn-secondary" type="button"
                        data-action="history"
                        data-item-id="${escapeHtml(item._id)}"
                        data-product-name="${escapeHtml(item.name)}">
                  History
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    `;
  };

  const buildModalsHtml = (item) => {
    const domId = safeDomId(item._id);
    const managerName = item.manager_name || '';
    const branchName = item.branch_name || '';
    const qty = Number(item.qty || 0);
    const legacyPrice = item.price;
    const sellingPrice = item.selling_price !== null && item.selling_price !== undefined
      ? Number(item.selling_price)
      : (item.price !== null && item.price !== undefined ? Number(item.price) : null);
    const costPrice = item.cost_price !== null && item.cost_price !== undefined ? Number(item.cost_price) : null;
    const calcMargin = (sellingPrice !== null && costPrice !== null) ? (sellingPrice - costPrice) : null;
    const marginValue = calcMargin !== null ? calcMargin.toFixed(2) : '';
    const branchesOptions = BRANCHES.map((branch, idx) => `
      <div class="form-check me-2">
        <input class="form-check-input branch-option" type="checkbox" name="branches" value="${escapeHtml(branch)}" id="editBranch${domId}${idx}">
        <label class="form-check-label small" for="editBranch${domId}${idx}">${escapeHtml(branch)}</label>
      </div>
    `).join('');

    const transferOptions = BRANCHES.filter((branch) => branch !== branchName).map((branch) => (
      `<option value="${escapeHtml(branch)}">${escapeHtml(branch)}</option>`
    )).join('');

    return `
      <div class="modal fade" id="changeImageModal${domId}" tabindex="-1" aria-hidden="true">
        <div class="modal-dialog">
          <div class="modal-content">
            <form method="POST" enctype="multipart/form-data" class="change-image-form" data-name="${escapeHtml(item.name)}" data-price="${escapeHtml(item.price ?? '')}">
              <div class="modal-header">
                <h5 class="modal-title">Change Image for "${escapeHtml(item.name)}"</h5>
                <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
              </div>
              <div class="modal-body">
                <div class="alert alert-danger d-none small modal-error" role="alert"></div>
                <input type="hidden" name="name" value="${escapeHtml(item.name)}">
                <input type="hidden" name="price" value="${escapeHtml(item.price ?? '')}">
                <input type="file" name="image" class="form-control mb-2" accept="image/*" required>
                <p class="text-muted small mb-1">New image will apply to all items with this name and price / selling price.</p>
                <div class="preview-container mt-2 text-center small text-muted">No image selected yet.</div>
              </div>
              <div class="modal-footer">
                <button type="submit" class="btn btn-primary">Update Image</button>
              </div>
            </form>
          </div>
        </div>
      </div>

      ${managerName !== 'Multiple' ? `
      <div class="modal fade" id="transferModal${domId}" tabindex="-1" aria-hidden="true">
        <div class="modal-dialog">
          <div class="modal-content">
            <form method="POST" class="transfer-item-form" data-item="${escapeHtml(item._id)}">
              <div class="modal-header">
                <h5 class="modal-title">Transfer ${escapeHtml(item.name)}</h5>
                <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
              </div>
              <div class="modal-body">
                <div class="alert alert-danger d-none small modal-error" role="alert"></div>
                <input type="hidden" name="item_id" value="${escapeHtml(item._id)}">
                <input type="hidden" name="action" value="transfer">
                <div class="mb-2">
                  <label class="form-label small text-muted">From Branch</label>
                  <input type="text" class="form-control" value="${escapeHtml(branchName)}" readonly>
                </div>
                <div class="mb-2">
                  <label class="form-label small text-muted">Destination Branch</label>
                  <select name="to_branch" class="form-select" required>
                    <option value="">Select branch</option>
                    ${transferOptions}
                  </select>
                </div>
                <div class="mb-2">
                  <label class="form-label small text-muted">Quantity to Transfer</label>
                  <input type="number" name="transfer_qty" class="form-control" min="1" max="${qty}" required>
                  <small class="small-help">Available in ${escapeHtml(branchName)}: ${qty}</small>
                </div>
                <div class="alert alert-info py-2 mt-2 small mb-0">
                  This will subtract from <strong>${escapeHtml(branchName)}</strong> and add to the selected branch for
                  <strong>${escapeHtml(item.name)}</strong>.
                </div>
              </div>
              <div class="modal-footer">
                <button type="submit" class="btn btn-transfer">Confirm Transfer</button>
              </div>
            </form>
          </div>
        </div>
      </div>
      ` : ''}

      <div class="modal fade" id="editModal${domId}" tabindex="-1" aria-hidden="true">
        <div class="modal-dialog">
          <div class="modal-content">
            <form method="POST" class="edit-item-form" data-item="${domId}">
              <div class="modal-header">
                <h5 class="modal-title">Edit ${escapeHtml(item.name)}</h5>
                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
              </div>
              <div class="modal-body">
                <div class="alert alert-danger d-none small modal-error" role="alert"></div>
                <input type="hidden" name="item_id" value="${escapeHtml(item._id)}">

                <div class="mb-3">
                  <label for="name${domId}" class="form-label">Product Name</label>
                  <input type="text" name="name" class="form-control" id="name${domId}" value="${escapeHtml(item.name)}" required>
                </div>

                <div class="row g-2">
                  <div class="col-md-4">
                    <label class="form-label" for="price${domId}">Legacy Price</label>
                    <input type="number" step="0.01" min="0" name="price" id="price${domId}" class="form-control" value="${formatMoney(legacyPrice, '')}">
                  </div>
                  <div class="col-md-4">
                    <label class="form-label" for="cost${domId}">Cost Price</label>
                    <input type="number" step="0.01" min="0" name="cost_price" id="cost${domId}" class="form-control" value="${formatMoney(costPrice, '')}">
                  </div>
                  <div class="col-md-4">
                    <label class="form-label" for="sell${domId}">Selling Price</label>
                    <input type="number" step="0.01" min="0" name="selling_price" id="sell${domId}" class="form-control" value="${formatMoney(sellingPrice, '')}">
                  </div>
                  <div class="col-md-4">
                    <label class="form-label" for="margin${domId}">Margin</label>
                    <input type="number" step="0.01" name="margin" id="margin${domId}" class="form-control" value="${marginValue}" readonly>
                    <div class="form-text">Auto: Selling - Cost</div>
                  </div>
                </div>

                <div class="mb-3 mt-2">
                  <label for="qty${domId}" class="form-label">Quantity</label>
                  <input type="number" name="qty" class="form-control" id="qty${domId}" value="${qty}" required>
                </div>

                <div class="mb-3">
                  <label class="form-label">Apply to Branches</label>
                  <div class="d-flex flex-wrap">
                    <div class="form-check me-2">
                      <input class="form-check-input branch-master" type="checkbox" id="editAllBranches${domId}">
                      <label class="form-check-label small" for="editAllBranches${domId}">All</label>
                    </div>
                    ${branchesOptions}
                  </div>
                  <small class="text-muted">Tick all the branches you want the update to apply to.</small>
                </div>
              </div>
              <div class="modal-footer">
                <button type="submit" name="action" value="update" class="btn btn-primary">Save changes</button>
              </div>
            </form>
          </div>
        </div>
      </div>
    `;
  };

  const updateLoadMore = () => {
    if (!loadMoreWrap || !loadMoreBtn) return;
    loadMoreWrap.style.display = state.has_more ? 'block' : 'none';
    if (!loadMoreMeta) return;
    const shown = Math.min(state.offset, state.total_count);
    if (!state.total_count) {
      loadMoreMeta.textContent = '';
      return;
    }
    const remaining = Math.max(state.total_count - shown, 0);
    loadMoreMeta.textContent = remaining ? `${remaining} remaining` : `Showing ${shown} of ${state.total_count}`;
  };

  const loadInventory = async ({ reset } = {}) => {
    if (reset) {
      state.offset = 0;
      renderSkeletons();
    }
    if (loadMoreBtn) {
      loadMoreBtn.disabled = true;
    }
    const params = new URLSearchParams({
      manager: state.manager || '',
      branch: state.branch || '',
      product: state.product || '',
      low_stock: state.low_stock ? '1' : '',
      limit: String(state.limit),
      offset: String(state.offset)
    });
    try {
      const resp = await fetch(`${LIST_ENDPOINT}?${params.toString()}`);
      const data = await resp.json();
      if (!resp.ok || !data.ok) {
        throw new Error((data && data.message) || 'Failed to load inventory.');
      }
      state.total_count = data.total_count || 0;
      state.has_more = !!data.has_more;
      state.group_products = !!data.group_products;
      state.reorder_level = parseInt(data.reorder_level || state.reorder_level, 10) || state.reorder_level;
      if (reset) {
        inventoryGrid.innerHTML = '';
        if (modalsContainer) modalsContainer.innerHTML = '';
      }
      const items = Array.isArray(data.items) ? data.items : [];
      if (!items.length && reset) {
        inventoryGrid.innerHTML = '<div class="col"><div class="alert alert-warning">No items found.</div></div>';
      } else {
        items.forEach((item) => {
          inventoryGrid.insertAdjacentHTML('beforeend', buildCardHtml(item));
          if (modalsContainer) {
            modalsContainer.insertAdjacentHTML('beforeend', buildModalsHtml(item));
          }
          if (typeof window.hookMarginCalcFor === 'function') {
            window.hookMarginCalcFor(safeDomId(item._id));
          }
        });
      }
      state.offset = (data.offset || 0) + items.length;
      updateLoadMore();
    } catch (err) {
      if (reset) {
        inventoryGrid.innerHTML = '<div class="col"><div class="alert alert-danger">Failed to load inventory.</div></div>';
      }
      showToast('danger', err.message || 'Failed to load inventory.');
    } finally {
      if (loadMoreBtn) {
        loadMoreBtn.disabled = false;
      }
    }
  };

  const updateCardDom = (item) => {
    if (!item || !item._id) return false;
    const card = document.querySelector(`.card[data-item-id="${escapeSelectorFn(item._id)}"]`);
    if (!card) return false;
    if (item.name !== undefined) card.dataset.productName = item.name || '';
    if (item.qty !== undefined) card.dataset.qty = item.qty || 0;
    if (item.price !== undefined) card.dataset.price = item.price ?? '';
    if (item.selling_price !== undefined) card.dataset.sellingPrice = item.selling_price ?? '';
    if (item.cost_price !== undefined) card.dataset.costPrice = item.cost_price ?? '';
    if (item.description !== undefined) card.dataset.description = item.description || '';
    if (item.image_url !== undefined) card.dataset.imageUrl = item.image_url || '';
    const title = card.querySelector('.card-title');
    if (title && item.name !== undefined) title.textContent = item.name || '';
    const qtyBadge = card.querySelector('.qty-badge');
    if (qtyBadge && item.qty !== undefined) qtyBadge.textContent = Number(item.qty || 0).toFixed(0);
    const pricePill = card.querySelector('.price-pill');
    if (pricePill && item.price !== undefined) pricePill.textContent = `GHS ${formatMoney(item.price, '0.00')}`;
    const stockStrong = card.querySelector('.badge strong');
    if (stockStrong && item.qty !== undefined) stockStrong.textContent = Number(item.qty || 0).toFixed(0);
    const image = card.querySelector('img.card-img-top');
    if (image && item.image_url) image.src = item.image_url;
    if (item.description !== undefined) {
      const desc = card.querySelector('.product-details .card-text.text-muted-quiet');
      if (desc) desc.textContent = item.description || '';
    }
    const selling = item.selling_price !== undefined
      ? (item.selling_price !== null && item.selling_price !== undefined ? Number(item.selling_price) : null)
      : null;
    const cost = item.cost_price !== undefined
      ? (item.cost_price !== null && item.cost_price !== undefined ? Number(item.cost_price) : null)
      : null;
    const legacy = item.price !== undefined ? item.price : null;
    const qtyVal = item.qty !== undefined ? Number(item.qty || 0) : null;
    const gridChips = card.querySelectorAll('.price-grid .price-chip strong');
    if (gridChips.length >= 4) {
      if (legacy !== null) gridChips[0].textContent = formatMoney(legacy, '0.00');
      if (qtyVal !== null) gridChips[1].textContent = Number(qtyVal || 0).toFixed(0);
      if (selling !== null || item.selling_price !== undefined) gridChips[2].textContent = formatMoney(selling);
      if (cost !== null || item.cost_price !== undefined) gridChips[3].textContent = formatMoney(cost);
    }
    if (selling !== null && cost !== null) {
      const calcMargin = selling - cost;
      const pctMargin = cost ? (calcMargin / cost * 100) : null;
      const marginWrap = card.querySelector('.product-details .mt-2');
      if (marginWrap) {
        if (calcMargin > 0) {
          marginWrap.innerHTML = `<span class="badge-pos">Margin: GHS ${calcMargin.toFixed(2)}${pctMargin !== null ? ` (${pctMargin.toFixed(1)}%)` : ''}</span>`;
        } else if (calcMargin < 0) {
          marginWrap.innerHTML = `<span class="badge-neg">Margin: GHS ${calcMargin.toFixed(2)}${pctMargin !== null ? ` (${pctMargin.toFixed(1)}%)` : ''}</span>`;
        } else {
          marginWrap.innerHTML = '<span class="badge-zero">Margin: GHS 0.00</span>';
        }
      }
    }
    const lowStock = item.is_low_stock !== undefined ? !!item.is_low_stock : card.classList.contains('low-stock');
    card.classList.toggle('low-stock', lowStock);
    card.classList.toggle('low-stock-pulse', lowStock);
    const existingLow = card.querySelector('.low-stock-pill');
    if (lowStock && !existingLow) {
      const meta = card.querySelector('.compact-meta');
      if (meta) meta.insertAdjacentHTML('beforeend', '<span class="low-stock-pill">LOW STOCK</span>');
    } else if (!lowStock && existingLow) {
      existingLow.remove();
    }
    return true;
  };

  const removeCardDom = (itemId) => {
    const card = document.querySelector(`.card[data-item-id="${escapeSelectorFn(itemId)}"]`);
    if (card) {
      const col = card.closest('.col');
      if (col) col.remove();
      else card.remove();
    }
    const domId = safeDomId(itemId);
    ['changeImageModal', 'transferModal', 'editModal'].forEach((prefix) => {
      const modal = document.getElementById(`${prefix}${domId}`);
      if (modal) modal.remove();
    });
  };

  if (filterForm) {
    filterForm.addEventListener('submit', (e) => {
      e.preventDefault();
      getFiltersFromForm();
      saveFilters();
      updateUrlFromState();
      loadInventory({ reset: true });
    });
  }

  if (resetBtn) {
    resetBtn.addEventListener('click', () => {
      resetFilters();
      loadInventory({ reset: true });
    });
  }

  if (loadMoreBtn) {
    loadMoreBtn.addEventListener('click', () => {
      loadInventory({ reset: false });
    });
  }

  document.addEventListener('click', (event) => {
    const toggleBtn = event.target.closest('.toggle-details');
    if (toggleBtn) {
      event.preventDefault();
      const targetId = toggleBtn.getAttribute('data-target');
      if (!targetId) return;
      const panel = document.getElementById(targetId);
      if (!panel) return;
      const isOpen = panel.classList.toggle('is-open');
      toggleBtn.classList.toggle('is-open', isOpen);
      toggleBtn.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
      toggleBtn.setAttribute('title', isOpen ? 'Hide details' : 'Show more');
      return;
    }

    const productToggle = event.target.closest('.product-toggle');
    if (productToggle) {
      const card = productToggle.closest('.card');
      if (!card) return;
      const btn = card.querySelector('.toggle-details');
      if (btn) btn.click();
      return;
    }

    const historyBtn = event.target.closest('[data-action="history"]');
    if (historyBtn) {
      const itemId = historyBtn.dataset.itemId;
      const productName = historyBtn.dataset.productName || '';
      if (itemId && typeof window.openHistoryModal === 'function') {
        window.openHistoryModal(itemId, productName);
      }
    }
  });

  document.addEventListener('change', (event) => {
    const master = event.target.closest('.branch-master');
    if (master) {
      const container = master.closest('.modal-body') || master.closest('form');
      if (!container) return;
      const checkboxes = container.querySelectorAll('.branch-option');
      checkboxes.forEach((cb) => { cb.checked = master.checked; });
      return;
    }

    const fileInput = event.target;
    if (fileInput && fileInput.type === 'file') {
      const form = fileInput.closest('.change-image-form');
      if (!form) return;
      const previewContainer = form.querySelector('.preview-container');
      const file = fileInput.files[0];
      if (!previewContainer) return;
      if (!file) {
        previewContainer.textContent = 'No image selected yet.';
        return;
      }
      const imgPreview = document.createElement('img');
      imgPreview.src = URL.createObjectURL(file);
      imgPreview.style.maxHeight = '150px';
      imgPreview.classList.add('img-fluid', 'mt-2', 'rounded', 'border');
      previewContainer.innerHTML = '';
      previewContainer.appendChild(imgPreview);
    }
  });

  document.addEventListener('submit', async (event) => {
    const form = event.target;
    if (!form) return;

    if (form.classList.contains('edit-item-form')) {
      event.preventDefault();
      event.stopPropagation();
      setModalError(form, '');
      const itemId = (form.querySelector('[name="item_id"]') || {}).value;
      const branches = Array.from(form.querySelectorAll('input[name="branches"]:checked')).map((cb) => cb.value);
      if (!branches.length) {
        setModalError(form, 'Please select at least one branch.');
        return;
      }
      const payload = {
        item_id: itemId,
        branches,
        name: (form.querySelector('[name="name"]') || {}).value,
        price: (form.querySelector('[name="price"]') || {}).value,
        qty: (form.querySelector('[name="qty"]') || {}).value,
        cost_price: (form.querySelector('[name="cost_price"]') || {}).value,
        selling_price: (form.querySelector('[name="selling_price"]') || {}).value,
        expiry_date: (form.querySelector('[name="expiry_date"]') || {}).value
      };
      try {
        const resp = await fetch(UPDATE_ENDPOINT, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
          throw new Error((data && data.message) || 'Update failed.');
        }
        if (data.updated_item) {
          updateCardDom(data.updated_item);
        }
        showToast('success', `Product updated across ${data.updated_count || 0} branch(es).`);
        const modalEl = form.closest('.modal');
        if (modalEl && window.bootstrap) {
          bootstrap.Modal.getOrCreateInstance(modalEl).hide();
        }
      } catch (err) {
        setModalError(form, err.message || 'Update failed.');
        showToast('danger', err.message || 'Update failed.');
      }
      return;
    }

    if (form.classList.contains('delete-item-form')) {
      event.preventDefault();
      event.stopPropagation();
      if (!window.confirm('Are you sure you want to delete this item in selected branches?')) return;
      const itemId = (form.querySelector('[name="item_id"]') || {}).value;
      const branches = Array.from(form.querySelectorAll('input[name="branches"]:checked')).map((cb) => cb.value);
      if (!branches.length) {
        showToast('warning', 'Please select at least one branch.');
        return;
      }
      try {
        const resp = await fetch(DELETE_ENDPOINT, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ item_id: itemId, branches })
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
          throw new Error((data && data.message) || 'Delete failed.');
        }
        removeCardDom(itemId);
        showToast('success', `Product deleted across ${data.deleted_count || 0} branch(es).`);
      } catch (err) {
        showToast('danger', err.message || 'Delete failed.');
      }
      return;
    }

    if (form.classList.contains('transfer-item-form')) {
      event.preventDefault();
      event.stopPropagation();
      setModalError(form, '');
      const itemId = (form.querySelector('[name="item_id"]') || {}).value;
      const transferQty = (form.querySelector('[name="transfer_qty"]') || {}).value;
      const toBranch = (form.querySelector('[name="to_branch"]') || {}).value;
      try {
        const resp = await fetch(TRANSFER_ENDPOINT, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ item_id: itemId, transfer_qty: transferQty, to_branch: toBranch })
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
          throw new Error((data && data.message) || 'Transfer failed.');
        }
        updateCardDom({
          _id: itemId,
          qty: data.new_src_qty,
          is_low_stock: data.new_src_qty <= state.reorder_level
        });
        showToast('success', `Transferred ${transferQty} unit(s) to ${data.to_branch}.`);
        const modalEl = form.closest('.modal');
        if (modalEl && window.bootstrap) {
          bootstrap.Modal.getOrCreateInstance(modalEl).hide();
        }
      } catch (err) {
        setModalError(form, err.message || 'Transfer failed.');
        showToast('danger', err.message || 'Transfer failed.');
      }
      return;
    }

    if (form.classList.contains('change-image-form')) {
      event.preventDefault();
      event.stopPropagation();
      setModalError(form, '');
      const modalEl = form.closest('.modal');
      const itemId = modalEl ? (modalEl.id || '').replace('changeImageModal', '') : '';
      if (!itemId) {
        setModalError(form, 'Missing item ID.');
        return;
      }
      const submitBtn = form.querySelector('button[type="submit"]');
      if (submitBtn) submitBtn.disabled = true;
      const formData = new FormData(form);
      try {
        const resp = await fetch(`${CHANGE_IMAGE_BASE}/${encodeURIComponent(itemId)}`, {
          method: 'POST',
          body: formData
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
          throw new Error((data && data.message) || 'Image update failed.');
        }
        document.querySelectorAll('.card[data-product-name]').forEach((card) => {
          if (card.dataset.productName === form.dataset.name) {
            const img = card.querySelector('img.card-img-top');
            if (img) img.src = data.image_url;
          }
        });
        showToast('success', `Image updated for ${data.updated_count || 0} item(s).`);
        if (modalEl && window.bootstrap) {
          bootstrap.Modal.getOrCreateInstance(modalEl).hide();
        }
      } catch (err) {
        setModalError(form, err.message || 'Image update failed.');
        showToast('danger', err.message || 'Image update failed.');
      } finally {
        if (submitBtn) submitBtn.disabled = false;
      }
    }
  }, true);

  restoreFilters();
  loadInventory({ reset: true });
})();
