from flask import Flask, redirect, url_for, send_from_directory
from flask_login import LoginManager, logout_user, login_required
from flask_bcrypt import Bcrypt
import os

# Shared Utility
from user_model import get_user_by_id

# ---------------- Admin Blueprints ----------------
from login import login_bp
from register_manager import register_manager_bp
from managers import managers_bp
from add_inventory import add_inventory_bp
from add_product import add_product_bp
from inventory_products import inventory_products_bp
from inventory_analysis import inventory_analysis_bp
from recruitment import recruitment_bp
from admin_topups import admin_topups_bp
from admin_tasks import admin_task_bp
from admin_accountability import admin_account_bp
from admin_close_account import admin_close_account_bp
from account_summary_analysis import account_summary_analysis_bp
from chat import chat_bp
from login_logs import login_logs_bp
from added_products import added_products_bp
from todo import todo_bp
from product_profile import product_profile_bp
from executive_dashboard import executive_bp
from executive_sales import executive_sales_bp
from executive_customers import executive_customers_bp
from manager_target import manager_target_bp
from executive_view_customer import executive_view_bp
from executive_agent_target import executive_agent_target_bp
from sales_close_agent import sales_close_agent_bp
from inventory_dashboard import inventory_dashboard
from routes.inventory.profile import inventory_profile_bp
from executive_profile import executive_profile_bp
from routes.admin_complaints import admin_complaints_bp
from routes.meeting_report import meeting_report_bp
from routes.agent_complaints import agent_complaints_bp
from routes.agent_sidebar import agent_sidebar_bp
from routes.inventory.orders import inventory_orders_bp  # Inventory Orders
from api_smartliving import api_bp

# ---------------- Manager Blueprints ----------------
from register import register_bp
from transfer import transfer_bp
from sales_summary import sales_summary_bp
from customers import customers_bp
from payments import payments_bp
from account import account_bp
from account_summary import account_summary_bp
from agents_report import agents_report_bp
from agent_lead import agent_lead_bp
from manager_products import manager_product_bp
from manager_analysis import manager_analysis_bp
from sold_products import sold_products_bp
from manager_profile import manager_profile_bp
from task_messages import task_messages_bp
from agent import agent_bp
from manager_inventory import manager_inventory_bp
from manager_inventory_analysis import manager_inventory_analysis_bp
from manager_view_admin_tasks import admin_task_view_bp
from view_targets import view_targets_bp
from executive_tasks import executive_task_bp
from routes.manager.orders import manager_orders_bp  # Manager Orders

# ---------------- Agent Blueprints ----------------
from dashboard_agent import agent_dashboard_bp
from agent_profile import agent_profile_bp
from customer import customer_bp
from view import view_bp
from sell import sell_bp
from product import product_bp
from payment import payment_bp
from lead import lead_bp
from analysis import analysis_bp
from report import report_bp
from assigned_products import assigned_products_bp
from agent_tasks import agent_tasks_bp
from agent_account import agent_account_bp
from set_target import target_bp
from manager_dashboard import manager_dashboard_bp
from manager_sales_close import manager_sales_close_bp
from admin_sales_close import admin_sales_close_bp
from executive_sales_close import executive_sales_close_bp
from transfer_product import transfer_product_bp
from close_card import close_card_bp
from packages import packages_bp
from routes.executive_pricing import executive_pricing_bp
from assign_products import assign_bp
from manager_deposits import manager_deposits_bp
from routes.transfer_customer import transfer_customer_bp
from executive_deposits import executive_deposits_bp
from manager_expense import manager_expense_bp
from executive_expense import executive_expense_bp
from routes.admin_transfer_customer import admin_transfer_customer_bp
from routes.complaints import complaints_bp
from routes.admin_profile import admin_profile_bp
from routes.admin_dashboard import admin_dashboard_bp
from routes.admin_transfer_logs import admin_transfer_logs_bp
from routes.manager_meeting_report import manager_meeting_report_bp

# ---------------- Accounting Blueprints (TTS) ----------------
from accounting_routes.accounts import accounting_bp                 # Chart of Accounts (CoA)
from accounting_routes.journals import journals_bp                   # Journals
from accounting_routes.ledger import ledger_bp                       # General Ledger

# 👉 Accounting clients blueprint (AR "customers")
# Blueprint object inside this module is named `customers_bp`, but
# we alias it here as `acc_clients_bp` and register with name="acc_clients"
from accounting_routes.customers import customers_bp as acc_clients_bp

from accounting_routes.ar_invoices import ar_invoices_bp             # AR Invoices
from accounting_routes.ar_payments import ar_payments_bp             # AR Payments
from accounting_routes.ar_aging import ar_aging_bp                   # AR Aging
from accounting_routes.ap_bills import ap_bills_bp                   # AP Bills
from accounting_routes.bank_accounts import bank_accounts_bp as acc_bank_accounts_bp  # Bank accounts
from accounting_routes.bank_recon import bank_recon_bp               # Bank Reconciliation
from accounting_routes.fixed_assets import fixed_assets_bp           # Fixed Assets
from accounting_routes.payroll_calculator import acc_payroll_calc    # Payroll
from accounting_routes.expenses import acc_expenses                  # Expenses
from accounting_routes.balance_sheet import acc_balance_sheet        # Balance Sheet
from accounting_routes.dashboard import acc_dashboard                # Accounting Dashboard
from accounting_routes.profile import acc_profile                    # Accounting Profile / Settings
from accounting_routes.payment_voucher import payment_voucher_bp     # Payment Vouchers
from accounting_routes.profit_loss import profit_loss_bp
from accounting_routes.budget import acc_budget
from routes.manager_susu import manager_susu_bp
from routes.executive_susu import executive_susu_bp
from routes.executive_stock_entry import executive_stock_entry_bp



# ---------------- App & Auth Setup ----------------
app = Flask(__name__)
app.secret_key = "supersecretkey"

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login.login"

bcrypt = Bcrypt(app)


@login_manager.user_loader
def load_user(user_id):
    return get_user_by_id(user_id)


# ---------------- Shared Routes ----------------
app.register_blueprint(login_bp)
app.register_blueprint(register_bp)

# ---------------- Admin Blueprints Registration ----------------
app.register_blueprint(register_manager_bp)
app.register_blueprint(managers_bp)
app.register_blueprint(add_inventory_bp)
app.register_blueprint(add_product_bp)
app.register_blueprint(inventory_products_bp)
app.register_blueprint(inventory_analysis_bp)
app.register_blueprint(recruitment_bp)
app.register_blueprint(admin_topups_bp)
app.register_blueprint(admin_task_bp)
app.register_blueprint(admin_account_bp)
app.register_blueprint(admin_close_account_bp)
app.register_blueprint(account_summary_analysis_bp)
app.register_blueprint(chat_bp)
app.register_blueprint(login_logs_bp)
app.register_blueprint(added_products_bp, url_prefix="/added_products")
app.register_blueprint(todo_bp)
app.register_blueprint(product_profile_bp)
app.register_blueprint(executive_bp)
app.register_blueprint(executive_sales_bp)
app.register_blueprint(manager_dashboard_bp)
app.register_blueprint(executive_customers_bp)
app.register_blueprint(manager_target_bp)
app.register_blueprint(executive_view_bp)
app.register_blueprint(executive_task_bp)
app.register_blueprint(executive_agent_target_bp)
app.register_blueprint(sales_close_agent_bp)
app.register_blueprint(manager_sales_close_bp)
app.register_blueprint(admin_sales_close_bp)
app.register_blueprint(executive_sales_close_bp)
app.register_blueprint(inventory_dashboard)
app.register_blueprint(transfer_product_bp)
app.register_blueprint(close_card_bp)
app.register_blueprint(assign_bp)
app.register_blueprint(manager_deposits_bp)
app.register_blueprint(executive_deposits_bp)
app.register_blueprint(manager_expense_bp)
app.register_blueprint(executive_expense_bp)
app.register_blueprint(inventory_profile_bp)
app.register_blueprint(executive_profile_bp)
app.register_blueprint(executive_pricing_bp)
app.register_blueprint(complaints_bp)
app.register_blueprint(meeting_report_bp)
app.register_blueprint(agent_complaints_bp)
app.register_blueprint(agent_sidebar_bp)
app.register_blueprint(transfer_customer_bp)
app.register_blueprint(admin_transfer_customer_bp)
app.register_blueprint(manager_meeting_report_bp)
app.register_blueprint(admin_profile_bp)
app.register_blueprint(admin_complaints_bp)
app.register_blueprint(admin_dashboard_bp)
app.register_blueprint(admin_transfer_logs_bp)
app.register_blueprint(api_bp)

# Inventory Orders
app.register_blueprint(inventory_orders_bp)

# ---------------- Manager Blueprints Registration ----------------
app.register_blueprint(transfer_bp)
app.register_blueprint(sales_summary_bp)
app.register_blueprint(customers_bp)
app.register_blueprint(payments_bp)
app.register_blueprint(account_bp)
app.register_blueprint(account_summary_bp)
app.register_blueprint(agents_report_bp)
app.register_blueprint(agent_lead_bp)
app.register_blueprint(manager_product_bp)
app.register_blueprint(manager_analysis_bp)
app.register_blueprint(sold_products_bp, url_prefix="/sold_products")
app.register_blueprint(manager_profile_bp)
app.register_blueprint(task_messages_bp)
app.register_blueprint(agent_bp)
app.register_blueprint(manager_inventory_bp)
app.register_blueprint(manager_inventory_analysis_bp)
app.register_blueprint(admin_task_view_bp)
app.register_blueprint(view_targets_bp)
app.register_blueprint(profit_loss_bp)   # maybe with url_prefix="/accounting" if you use that
app.register_blueprint(acc_budget, url_prefix="/accounting")
app.register_blueprint(manager_susu_bp)

# Manager Orders
app.register_blueprint(manager_orders_bp)

# ---------------- Agent Blueprints Registration ----------------
app.register_blueprint(agent_dashboard_bp)
app.register_blueprint(agent_profile_bp)
app.register_blueprint(view_bp, url_prefix="/view")
app.register_blueprint(customer_bp, url_prefix="/customer")
app.register_blueprint(product_bp, url_prefix="/product")
app.register_blueprint(sell_bp, url_prefix="/sell")
app.register_blueprint(payment_bp, url_prefix="/payment")
app.register_blueprint(report_bp, url_prefix="/report")
app.register_blueprint(lead_bp, url_prefix="/leads")
app.register_blueprint(analysis_bp, url_prefix="/analysis")
app.register_blueprint(assigned_products_bp, url_prefix="/sales")
app.register_blueprint(agent_tasks_bp, url_prefix="/agent/tasks")
app.register_blueprint(agent_account_bp)
app.register_blueprint(target_bp)
app.register_blueprint(packages_bp, url_prefix="/view")
app.register_blueprint(executive_stock_entry_bp)

# ---------------- Accounting Blueprints Registration ----------------
app.register_blueprint(ledger_bp, url_prefix="/accounting")

# Accounting Clients (AR clients) – endpoints: acc_clients.customers, acc_clients.quick_create
app.register_blueprint(
    acc_clients_bp,
    url_prefix="/accounting",
    name="acc_clients",
)  # /accounting/customers
app.register_blueprint(executive_susu_bp)

app.register_blueprint(ar_invoices_bp,    url_prefix="/accounting")  # /accounting/ar/invoices
app.register_blueprint(ar_payments_bp,    url_prefix="/accounting")  # /accounting/ar/payments
app.register_blueprint(ar_aging_bp,       url_prefix="/accounting")  # /accounting/ar/aging
app.register_blueprint(ap_bills_bp,       url_prefix="/accounting")  # /accounting/ap/bills
app.register_blueprint(acc_payroll_calc,  url_prefix="/accounting")  # /accounting/payroll
app.register_blueprint(acc_balance_sheet, url_prefix="/accounting")  # /accounting/balance-sheet

app.register_blueprint(
    acc_bank_accounts_bp,
    url_prefix="/accounting",
    name="acc_bank_accounts",
)  # /accounting/bank-accounts

app.register_blueprint(
    acc_dashboard,
    url_prefix="/accounting",  # /accounting/dashboard
)

app.register_blueprint(bank_recon_bp,      url_prefix="/accounting")          # /accounting/bank-recon
app.register_blueprint(accounting_bp,      url_prefix="/accounting")          # /accounting/accounts (CoA)
app.register_blueprint(journals_bp,        url_prefix="/accounting")          # /accounting/journals
app.register_blueprint(payment_voucher_bp, url_prefix="/accounting/payment-vouchers")

app.register_blueprint(
    fixed_assets_bp,
    url_prefix="/accounting/fixed-assets",  # /accounting/fixed-assets
)

app.register_blueprint(acc_expenses, url_prefix="/accounting")  # /accounting/expenses
app.register_blueprint(acc_profile)                             # uses its own prefix

# ---------------- File Uploads (Render Disk) ----------------
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
app.config.setdefault("UPLOADS_ROOT", os.path.join(BASE_DIR, "uploads"))
os.makedirs(app.config["UPLOADS_ROOT"], exist_ok=True)


@app.route("/uploads/<path:filename>")
def serve_uploaded_file(filename):
  return send_from_directory(app.config["UPLOADS_ROOT"], filename)


# ---------------- Root & Auth Shortcuts ----------------
@app.route("/")
def root():
  return redirect(url_for("login.login"))


@app.route("/logout")
@login_required
def logout():
  logout_user()
  return "Logged out."


if __name__ == "__main__":
  app.run(debug=True)

