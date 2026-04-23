# Extract values of time from Analysis of the Stated Preference Survey 2021 on Mode, Route and Departure Time Choices
import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import minimize
import mobility
import pandas as pd

# Define bins and average values
bin_edges = [0, 2, 10, 50, 80]  # Bin edges (in km)
average_values = {
    "VTT_voiture": [9, 12, 19, 27],
    "VTT_TP": [11, 15, 21, 24],
    "VTT_vélo": [28, 34, 37, 41],
    "VTT_marche": [14, 18, 23, 26]
}

# Define a function to compute the integral average of a linear segment over a bin
def compute_average(slope, intercept, x1, x2):
    integral = (slope / 2 * (x2**2 - x1**2)) + intercept * (x2 - x1)
    return integral / (x2 - x1)

# Objective function to minimize the error between model averages and table averages
def objective(params, bin_edges, averages):
    total_error = 0
    for i in range(len(bin_edges) - 1):
        x1, x2 = bin_edges[i], bin_edges[i + 1]
        slope, intercept = params[i * 2:i * 2 + 2]
        computed_avg = compute_average(slope, intercept, x1, x2)
        total_error += (computed_avg - averages[i])**2
    return total_error

# Constrain the continuity between segments
def continuity_constraint(params, bin_edges):
    constraints = []
    for i in range(len(bin_edges) - 2):
        x2 = bin_edges[i + 1]
        slope1, intercept1 = params[i * 2:i * 2 + 2]
        slope2, intercept2 = params[(i + 1) * 2:(i + 1) * 2 + 2]
        # Ensure the end of one segment matches the start of the next
        constraints.append(slope1 * x2 + intercept1 - (slope2 * x2 + intercept2))
    return constraints

# Positive slope constraint
def positive_slope_constraint(params):
    slopes = params[::2]  # Extract all slopes
    return slopes  # Must all be >= 0

# Fit models for each mode
piecewise_models = {}
for mode, averages in average_values.items():
    num_bins = len(bin_edges) - 1
    initial_guess = [0.1, np.mean(averages)] * num_bins  # Initial guess: small positive slope
    
    # Define constraints
    constraints = [
        {"type": "eq", "fun": lambda params: continuity_constraint(params, bin_edges)},
        {"type": "ineq", "fun": positive_slope_constraint},  # Enforce positive slopes
    ]
    
    # Optimize
    result = minimize(
        objective,
        initial_guess,
        args=(bin_edges, averages),
        constraints=constraints
    )
    
    piecewise_models[mode] = result.x  # Store optimized slopes and intercepts

# Plot the models
plt.figure(figsize=(10, 6))
x_vals = np.linspace(0, 100, 500)  # Extended range for visualization
for mode, params in piecewise_models.items():
    y_vals = []
    for i in range(len(bin_edges) - 1):
        x1, x2 = bin_edges[i], bin_edges[i + 1]
        slope, intercept = params[i * 2:i * 2 + 2]
        x_segment = x_vals[(x_vals >= x1) & (x_vals <= x2)]
        y_segment = slope * x_segment + intercept
        y_vals.extend(y_segment)
    plt.plot(x_vals[:len(y_vals)], y_vals, label=f"{mode}")

# Customize plot
plt.title("Constrained Piecewise Linear Models with Positive Slopes")
plt.xlabel("Distance (km)")
plt.ylabel("VTT (CHF/hour)")
plt.legend()
plt.grid()
plt.show()

for k, m in piecewise_models.items():
    print("---")
    print(k)
    print(m[7] + 80.0*m[6])
    
    
pt_cost_of_time = mobility.CostOfTimeParameters(
    intercept=11.0,
    breaks=[0.0, 2.0, 10.0, 50.0, 10000.0],
    slopes=[0.0, 1.0, 0.1, 0.067],
    max_value=25.0
)


walk_cost_of_time = mobility.CostOfTimeParameters(
    intercept=13.5,
    breaks=[0.0, 2.0, 10.0, 50.0, 10000.0],
    slopes=[0.484, 0.88, 0.074, 0.101],
    max_value=27.5
)

bicycle_cost_of_time = mobility.CostOfTimeParameters(
    intercept=23.5,
    breaks=[0.0, 2.0, 10.0, 50.0, 10000.0],
    slopes=[4.46, 0.385, 0.073, 0.169],
    max_value=43.5
)

car_cost_of_time = mobility.CostOfTimeParameters(
    intercept=7.7,
    breaks=[0.0, 2.0, 10.0, 50.0, 10000.0],
    slopes=[1.3, 0.424, 0.265, 0.18],
    max_value=29.7
)
    
    
pt_vot = pd.DataFrame({
    "mode": "public_transport",
    "distance": np.arange(0, 100, 1),
    "vot": pt_cost_of_time.compute(np.arange(0, 100, 1), "fr")
})

walk_vot = pd.DataFrame({
    "mode": "walk",
    "distance": np.arange(0, 100, 1),
    "vot": walk_cost_of_time.compute(np.arange(0, 100, 1), "fr")
})

bicycle_vot = pd.DataFrame({
    "mode": "bicycle",
    "distance": np.arange(0, 100, 1),
    "vot": bicycle_cost_of_time.compute(np.arange(0, 100, 1), "fr")
})

car_vot = pd.DataFrame({
    "mode": "car",
    "distance": np.arange(0, 100, 1),
    "vot": car_cost_of_time.compute(np.arange(0, 100, 1), "fr")
})

vot = pd.concat([pt_vot, walk_vot, bicycle_vot, car_vot])

import seaborn as sns
import matplotlib.pyplot as plt


fig, ax = plt.subplots(figsize=(8, 5), dpi=300)
sns.lineplot(data=vot, x="distance", y="vot", hue="mode", ax=ax)

plt.legend(title='Group', loc='upper center', bbox_to_anchor=(1, 0.5), ncol=1)
sns.move_legend(ax, "upper left", bbox_to_anchor=(1, 1))
plt.title("Value of time vs distance")
fig.subplots_adjust(right=0.8)
plt.tight_layout()
plt.show()