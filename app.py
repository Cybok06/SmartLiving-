from flask import Flask, redirect, url_for, send_from_directory, request
from flask_login import LoginManager
from flask_bcrypt import Bcrypt
from datetime import timedelta
import os

# Shared Util
from user_model import get_user_by_id
import hr_backend.hr_recruitment
import hr_backend.hr_wages

# ---------------- Admin Blueprints ----------------
from login import login_bp, get_current_identity
from config_constants import DEFAULT_PROFILE_IMAGE_URL
from services.activity_audit import ensure_activity_log_indexes, audit_request
from auth_password_reset import auth_password_reset_bp
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
from executive_users import exec_users_bp
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
from routes.manager_sidebar import manager_sidebar_bp
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
import hr_backend.hr_attendance
import hr_backend.hr_assets_debts
import hr_backend.hr_files               # ✅ NEW – HR Files routes
import hr_backend.hr_performance_cases   # ✅ NEW – Performance & Cases routes
# ---------------- Agent Blueprints ----------------
from dashboard_agent import agent_dashboard_bp
from agent_profile import agent_profile_bp
from customer import customer_bp
from view import view_bp
from sell import sell_bp
from hr_backend.employee_rating import employee_rating_bp
import hr_backend.hr_reminders   # ✅ NEW – Reminders routes
import hr_backend.hr_exits
from product import product_bp
from payment import payment_bp
# in app.py (or your blueprint loader)
from routes.manager_payroll import manager_payroll_bp

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
from routes.issues import issues_bp
from routes.undelivered_items import undelivered_items_bp
from routes.returns_inwards import returns_inwards_bp
from executive_inventory_analytics import executive_inventory_analytics_bp

# ---------------- Accounting Blueprints (TTS) ----------------
from accounting_routes.accounts import accounting_bp                 # Chart of Accounts (CoA)
from accounting_routes.journals import journals_bp                   # Journals
from accounting_routes.ledger import ledger_bp                       # General Ledger
from routes.hr_payroll import hr_payroll_bp

# 👉 Accounting clients blueprint (AR "customers")
# Blueprint object inside this module is named `customers_bp`, but
# we alias it here as `acc_clients_bp` and register with name="acc_clients"
from accounting_routes.customers import customers_bp as acc_clients_bp
from routes.agent_payroll import agent_payroll_bp

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
from accounting_routes.loans import loans_bp as acc_loans            # Loans (Long-term liability)
from accounting_routes.dashboard import acc_dashboard                # Accounting Dashboard
from accounting_routes.profile import acc_profile                    # Accounting Profile / Settings
from accounting_routes.payment_voucher import payment_voucher_bp     # Payment Vouchers
from accounting_routes.profit_loss import profit_loss_bp             # Profit & Loss
from accounting_routes.budget import acc_budget                      # Budgeting
from accounting_routes.private_ledger import private_ledger_bp       # Private Ledger
from accounting_routes.prepayments import prepayments_bp             # Prepayments
from accounting_routes.accruals import accruals_bp                   # Accruals / Owings

# SUSU / Stock entry
from routes.manager_susu import manager_susu_bp
from routes.executive_susu import executive_susu_bp
from routes.executive_stock_entry import executive_stock_entry_bp
from routes.executive_returns_outwards import executive_returns_outwards_bp
from routes.accounting_income import income_bp
from hr_backend.hr_dashboard import hr_bp
from routes.reports_insights import reports_insights_bp

# ---------------- HR Module (new) ----------------
# hr_dashboard defines the HR blueprint (hr_bp)
from hr_backend.hr_dashboard import hr_bp

# These modules attach routes to hr_bp via side effects
# ✅ NEW SPLIT: directory + add + recruitment + profile
import hr_backend.hr_employees_directory     # listing, filters, stats, export
import hr_backend.hr_employee_add            # add employee POST
import hr_backend.hr_employee_profile        # profile, overview, rating, sales level routes
import hr_backend.hr_roles                   # roles endpoints
import hr_backend.hr_profile                 # profile routes

# ---------------- App & Auth Setup ----------------
app = Flask(__name__)
app.secret_key = "MZeI7GiNW2bG1Q-1G1hy3Ax_MxLvab9DULjbFLTEuZU"
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=30)
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["REMEMBER_COOKIE_DURATION"] = timedelta(days=30)
app.config["REMEMBER_COOKIE_HTTPONLY"] = True
app.config["REMEMBER_COOKIE_SAMESITE"] = "Lax"
is_prod = app.config.get("ENV") == "production"
app.config["SESSION_COOKIE_SECURE"] = is_prod
app.config["REMEMBER_COOKIE_SECURE"] = is_prod

ensure_activity_log_indexes()

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login.login"

bcrypt = Bcrypt(app)


@login_manager.user_loader
def load_user(user_id):
    return get_user_by_id(user_id)


# ---------------- Shared Routes ----------------
app.register_blueprint(login_bp)
app.register_blueprint(auth_password_reset_bp)
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
app.register_blueprint(exec_users_bp)
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
app.register_blueprint(manager_sidebar_bp)
app.register_blueprint(transfer_customer_bp)
app.register_blueprint(reports_insights_bp)
app.register_blueprint(admin_transfer_customer_bp)
app.register_blueprint(manager_meeting_report_bp)
app.register_blueprint(admin_profile_bp)
app.register_blueprint(admin_complaints_bp)
app.register_blueprint(admin_dashboard_bp)
app.register_blueprint(admin_transfer_logs_bp)
app.register_blueprint(issues_bp)
app.register_blueprint(undelivered_items_bp)
app.register_blueprint(returns_inwards_bp)
app.register_blueprint(executive_inventory_analytics_bp)

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
app.register_blueprint(profit_loss_bp)   # /profit-loss or whatever is defined inside
app.register_blueprint(acc_budget, url_prefix="/accounting")
app.register_blueprint(manager_susu_bp)
app.register_blueprint(manager_payroll_bp)

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
app.register_blueprint(executive_returns_outwards_bp)
app.register_blueprint(income_bp)

# ---------------- Accounting Blueprints Registration ----------------
app.register_blueprint(ledger_bp, url_prefix="/accounting")

app.register_blueprint(acc_loans, url_prefix="/accounting")

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
app.register_blueprint(employee_rating_bp)
app.register_blueprint(private_ledger_bp, url_prefix="/accounting")  # /accounting/private-ledger
app.register_blueprint(prepayments_bp,    url_prefix="/accounting")  # /accounting/prepayments
app.register_blueprint(accruals_bp,       url_prefix="/accounting")  # /accounting/accruals

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
app.register_blueprint(api_bp)
app.register_blueprint(hr_payroll_bp)
app.register_blueprint(agent_payroll_bp)

app.register_blueprint(
    fixed_assets_bp,
    url_prefix="/accounting/fixed-assets",  # /accounting/fixed-assets
)

app.register_blueprint(acc_expenses, url_prefix="/accounting")  # /accounting/expenses
app.register_blueprint(acc_profile)                             # uses its own prefix

# ---------------- HR Blueprint Registration ----------------
# hr_bp already has its url_prefix defined inside hr_dashboard
app.register_blueprint(hr_bp)

# ---------------- File Uploads (Render Disk) ----------------

@app.template_filter("format_number")
def format_number(value, decimals: int = 0):
    try:
        dec = int(decimals or 0)
    except Exception:
        dec = 0
    try:
        num = float(value)
    except Exception:
        return value
    if dec <= 0:
        return f"{num:,.0f}"
    return f"{num:,.{dec}f}"


@app.template_filter("format_money")
def format_money(value):
    return format_number(value, 2)


@app.context_processor
def inject_loans_url():
    try:
        return {"url_loans": url_for("acc_loans.loans_page")}
    except Exception:
        return {"url_loans": None}

@app.context_processor
def inject_profile_image_default():
    return {"DEFAULT_PROFILE_IMAGE_URL": DEFAULT_PROFILE_IMAGE_URL}
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
app.config.setdefault("UPLOADS_ROOT", os.path.join(BASE_DIR, "uploads"))
os.makedirs(app.config["UPLOADS_ROOT"], exist_ok=True)


@app.route("/uploads/<path:filename>")
def serve_uploaded_file(filename):
    return send_from_directory(app.config["UPLOADS_ROOT"], filename)


# ---------------- Root & Auth Shortcuts ----------------
@app.route("/")
def root():
    ident = get_current_identity()
    if ident.get("is_authenticated"):
        return redirect(url_for(ident["dashboard_endpoint"]))
    return redirect(url_for("login.login"))


@app.after_request
def audit_mutations(response):
    audit_request(request, response)
    return response


if __name__ == "__main__":
    app.run(debug=True)
