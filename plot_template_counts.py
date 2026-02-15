#!/usr/bin/env python3
"""Draw bar plot for count column of bird23_canonical_templates.csv."""

import csv
import os
os.environ.setdefault("MPLBACKEND", "Agg")
os.environ.setdefault("MPLCONFIGDIR", os.path.join(os.path.dirname(__file__), ".mplconfig"))
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

CSV_PATH = os.path.join(os.path.dirname(__file__), "bird23_canonical_templates.csv")
OUT_PATH = os.path.join(os.path.dirname(__file__), "bird23_canonical_templates_count_barplot.png")

template_ids = []
counts = []
with open(CSV_PATH, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        try:
            template_ids.append(int(row["template_id"]))
            counts.append(int(row["count"]))
        except (ValueError, KeyError):
            continue

# Linear bar plot
plt.figure(figsize=(14, 5))
plt.bar(template_ids, counts, width=0.8, color="steelblue", edgecolor="none")
plt.xlabel("Template ID")
plt.ylabel("Count")
plt.title("Count per template (bird23_canonical_templates.csv)")
plt.tight_layout()
plt.savefig(OUT_PATH, dpi=150)
print(f"Saved {OUT_PATH}")
plt.close()

# Log-log scatter + regression line (power-law fit: count = a * template_id^b)
# Two fits: (1) exclude count=1 only; (2) exclude count=1 and count=2
OUT_PATH_LOGLOG = os.path.join(os.path.dirname(__file__), "bird23_canonical_templates_count_barplot_loglog.png")
import numpy as np
x = np.array(template_ids, dtype=float)
y = np.array(counts, dtype=float)

def power_law_fit(x_data, y_data):
    """Fit count = a * x^b in log space; return (a, b)."""
    log_x = np.log(x_data)
    log_y = np.log(y_data)
    slope, intercept = np.polyfit(log_x, log_y, 1)
    return np.exp(intercept), slope

# Fit 1: exclude only count=1
mask1 = y > 1
a1, b1 = power_law_fit(x[mask1], y[mask1])
# Fit 2: exclude count=1 and count=2
mask2 = y > 2
a2, b2 = power_law_fit(x[mask2], y[mask2])

# Evaluate both on same set: points with count > 2 (fair comparison)
eval_mask = y > 2
x_eval = x[eval_mask]
y_eval = y[eval_mask]
n_eval = eval_mask.sum()
y_pred1 = a1 * (x_eval ** b1)
y_pred2 = a2 * (x_eval ** b2)
# Losses: MSE in original space and in log space
mse_orig_1 = np.mean((y_eval - y_pred1) ** 2)
mse_orig_2 = np.mean((y_eval - y_pred2) ** 2)
mse_log_1 = np.mean((np.log(y_eval) - np.log(y_pred1)) ** 2)
mse_log_2 = np.mean((np.log(y_eval) - np.log(y_pred2)) ** 2)
rmse_orig_1 = np.sqrt(mse_orig_1)
rmse_orig_2 = np.sqrt(mse_orig_2)
rmse_log_1 = np.sqrt(mse_log_1)
rmse_log_2 = np.sqrt(mse_log_2)

# Plot: show data and fit 2 (count>2) as main curve
a, b = a2, b2
x_line = np.linspace(x.min(), x.max(), 200)
y_line = a * (x_line ** b)
plt.figure(figsize=(14, 5))
plt.scatter(x, y, s=8, color="steelblue", alpha=0.7, label="data")
plt.plot(x_line, y_line, color="coral", linewidth=2, label=f"fit (count>2): count = {a:.2f} × ID$^{{{b:.3f}}}$")
plt.xscale("log")
plt.yscale("log")
plt.xlabel("Template ID")
plt.ylabel("Count")
plt.title("Count per template (log-log), power-law fit excluding count=1 and count=2")
plt.legend()
plt.tight_layout()
plt.savefig(OUT_PATH_LOGLOG, dpi=150)
print(f"Saved {OUT_PATH_LOGLOG}")
plt.close()

# Report both fits and loss comparison
print("--- Fit 1: exclude count=1 only ---")
print(f"  a = {a1:.6f},  b = {b1:.6f}  =>  count = {a1:.4f} × template_id^{b1:.4f}")
print(f"  Fitted on n = {mask1.sum()} points")
print("--- Fit 2: exclude count=1 and count=2 ---")
print(f"  a = {a2:.6f},  b = {b2:.6f}  =>  count = {a2:.4f} × template_id^{b2:.4f}")
print(f"  Fitted on n = {mask2.sum()} points")
print("--- Loss comparison (evaluated on n = {} points with count > 2) ---".format(n_eval))
print("                    Fit 1 (count>1)   Fit 2 (count>2)")
print("  MSE (original)    {:16.2f}   {:16.2f}".format(mse_orig_1, mse_orig_2))
print("  RMSE (original)   {:16.2f}   {:16.2f}".format(rmse_orig_1, rmse_orig_2))
print("  MSE (log space)   {:16.6f}   {:16.6f}".format(mse_log_1, mse_log_2))
print("  RMSE (log space)  {:16.6f}   {:16.6f}".format(rmse_log_1, rmse_log_2))
better_orig = "Fit 2" if mse_orig_2 < mse_orig_1 else "Fit 1"
better_log = "Fit 2" if mse_log_2 < mse_log_1 else "Fit 1"
print("  Lower MSE (orig): {}   Lower MSE (log): {}".format(better_orig, better_log))

# Goodness-of-fit (same evaluation set: count > 2)
from scipy import stats
eps = 1e-10
def r2_orig(y_true, y_pred):
    ss_res = np.sum((y_true - y_pred) ** 2)
    ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
    return 1 - ss_res / ss_tot if ss_tot > 0 else np.nan
def r2_log(y_true, y_pred):
    log_y = np.log(y_true + eps)
    log_pred = np.log(np.maximum(y_pred, eps))
    ss_res = np.sum((log_y - log_pred) ** 2)
    ss_tot = np.sum((log_y - np.mean(log_y)) ** 2)
    return 1 - ss_res / ss_tot if ss_tot > 0 else np.nan
def chi2_gof(y_true, y_pred, df_offset=2):
    E = np.maximum(y_pred, eps)
    chi2 = np.sum((y_true - y_pred) ** 2 / E)
    df = len(y_true) - df_offset
    p_value = stats.chi2.sf(chi2, df) if df > 0 else np.nan
    return chi2, df, p_value

r2_orig_1 = r2_orig(y_eval, y_pred1)
r2_orig_2 = r2_orig(y_eval, y_pred2)
r2_log_1 = r2_log(y_eval, y_pred1)
r2_log_2 = r2_log(y_eval, y_pred2)
chi2_1, dof1, p1 = chi2_gof(y_eval, y_pred1)
chi2_2, dof2, p2 = chi2_gof(y_eval, y_pred2)
r_pearson_1 = np.corrcoef(y_eval, y_pred1)[0, 1] if np.std(y_pred1) > 0 else np.nan
r_pearson_2 = np.corrcoef(y_eval, y_pred2)[0, 1] if np.std(y_pred2) > 0 else np.nan
reduced_chi2_1 = chi2_1 / dof1 if dof1 else np.nan
reduced_chi2_2 = chi2_2 / dof2 if dof2 else np.nan

print("--- Goodness-of-fit (evaluation set: n = {} with count > 2) ---".format(n_eval))
print("                        Fit 1 (count>1)   Fit 2 (count>2)")
print("  R² (original)       {:16.4f}   {:16.4f}".format(r2_orig_1, r2_orig_2))
print("  R² (log space)      {:16.4f}   {:16.4f}".format(r2_log_1, r2_log_2))
print("  Pearson r           {:16.4f}   {:16.4f}".format(r_pearson_1, r_pearson_2))
print("  Chi-squared         {:16.2f}   {:16.2f}".format(chi2_1, chi2_2))
print("  Reduced χ² (χ²/df)  {:16.4f}   {:16.4f}".format(reduced_chi2_1, reduced_chi2_2))
print("  df                  {:16d}   {:16d}".format(int(dof1), int(dof2)))
print("  p-value (χ² GOF)    {:16.4e}   {:16.4e}".format(p1, p2))
print("  (Lower χ² / reduced χ² = better fit; high p = no evidence against model.)")
