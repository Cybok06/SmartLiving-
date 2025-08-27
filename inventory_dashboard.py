from flask import Blueprint, render_template, redirect, url_for, session

inventory_dashboard = Blueprint("inventory_dashboard", __name__, template_folder="templates")

@inventory_dashboard.route("/inventory/dashboard")
def inventory_dashboard_view():
    if "inventory_id" not in session:
        return redirect(url_for("login.login"))
    username = session.get("inventory_name") or session.get("username", "Inventory User")
    return render_template("inventory_dashboard.html", username=username)
