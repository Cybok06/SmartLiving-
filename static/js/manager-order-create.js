const rows = document.getElementById("rows");
const addBtn = document.getElementById("add");
const saveBtn = document.getElementById("save");
const notesField = document.getElementById("notes");

addBtn.onclick = () => WarehouseOrderPicker.addRow(rows);

async function submitWarehouseOrder() {
  const { items, errors } = WarehouseOrderPicker.gatherItems(rows);
  if (errors.length) {
    return showMessage(errors[0], "danger");
  }
  if (!items.length) {
    return showMessage("Add at least one valid warehouse product.", "danger");
  }

  const payload = {
    notes: notesField.value.trim(),
    items,
  };

  try {
    const res = await fetch("/manager/orders/create", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const json = await res.json();
    if (!json.ok) {
      throw new Error(json.message || "Failed to create order.");
    }
    rows.innerHTML = "";
    WarehouseOrderPicker.addRow(rows);
    notesField.value = "";
    showMessage("Order submitted successfully and sent to Inventory.", "success");
  } catch (err) {
    showMessage(err.message || "Failed to submit order.", "danger");
  }
}

saveBtn.onclick = submitWarehouseOrder;

function showMessage(msg, type = "success") {
  const wrap = document.getElementById("alertWrap");
  const box = document.getElementById("alertBox");
  box.className = `alert alert-${type}`;
  box.textContent = msg;
  wrap.style.display = "";
  setTimeout(() => (wrap.style.display = "none"), 4000);
}

WarehouseOrderPicker.addRow(rows);
