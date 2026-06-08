import csv
import os
import sys

# Try to import matplotlib library
try:
    import matplotlib.pyplot as plt
except ImportError:
    print("WARNING: WARNING: matplotlib library is not installed! You need to install it to draw charts:")
    print("   pip install matplotlib")
    sys.exit(1)

# Results directory and output directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(BASE_DIR, "results")

def load_csv_data(filename):
    """
    Reads data from the specified CSV file.
    """
    filepath = os.path.join(RESULTS_DIR, filename)
    if not os.path.exists(filepath):
        print(f"ERROR: Error: Data file not found: {filepath}")
        return None
        
    data = []
    with open(filepath, mode="r", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)
        for row in reader:
            data.append(row)
    return headers, data

def plot_eventual_consistency():
    """
    Experiment 1: Eventual Consistency - Replication Lag Plot
    """
    res = load_csv_data("eventual_results.csv")
    if not res:
        return
    headers, rows = res
    
    trials = [int(row[0]) for row in rows]
    lags = [float(row[6]) for row in rows]
    
    plt.figure(figsize=(10, 5))
    # Sleek harmonious color palette
    bars = plt.bar(trials, lags, color='#4A90E2', edgecolor='#2171C7', alpha=0.85, width=0.6)
    
    # Write the values above the columns
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2.0, height + 0.05, f"{height:.1f}ms", ha='center', va='bottom', fontsize=9, fontweight='bold')
        
    plt.title("Experiment 1: Eventual Consistency — Replication Lag", fontsize=13, fontweight='bold', pad=15)
    plt.xlabel("Iteration (Trial No)", fontsize=11, labelpad=10)
    plt.ylabel("Lag (Milliseconds - ms)", fontsize=11, labelpad=10)
    plt.xticks(trials)
    plt.grid(axis='y', linestyle='--', alpha=0.5)
    
    # Add average line
    avg_lag = sum(lags) / len(lags)
    plt.axhline(avg_lag, color='#D0021B', linestyle=':', linewidth=2, label=f"Average: {avg_lag:.2f} ms")
    plt.legend(loc="upper right", frameon=True, facecolor='white', edgecolor='none')
    
    plt.tight_layout()
    output_path = os.path.join(RESULTS_DIR, "eventual_lag_plot.png")
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"Chart generated: {output_path}")

def plot_monotonic_reads():
    """
    Experiment 2: Monotonic Reads - Version Progress Plot
    """
    res_single = load_csv_data("monotonic_results_single.csv")
    res_cross = load_csv_data("monotonic_results_cross.csv")
    
    if not res_single or not res_cross:
        return
        
    # Single node data
    _, rows_s = res_single
    reads_s = [int(row[0]) for row in rows_s[:20]]
    versions_s = [int(row[2]) for row in rows_s[:20]]
    
    # Multi-node data
    _, rows_c = res_cross
    reads_c = [int(row[0]) for row in rows_c[:20]]
    versions_l = [int(row[2]) for row in rows_c[:20]]
    versions_f = [int(row[3]) for row in rows_c[:20]]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # Left Plot: Single Node
    ax1.plot(reads_s, versions_s, marker='o', color='#2CA02C', linewidth=2.5, markersize=6, label="Follower Read")
    ax1.set_title("A. Single Node Read (Follower Only)\n[Monotonic Reads Preserved]", fontsize=12, fontweight='bold', pad=10)
    ax1.set_xlabel("Sequential Read Count", fontsize=10)
    ax1.set_ylabel("Read Version (Version)", fontsize=10)
    ax1.set_yticks(range(0, 13))
    ax1.grid(True, linestyle=':', alpha=0.6)
    ax1.legend(loc="lower right")
    
    # Right Plot: Multi-Node (Violation Scenario)
    ax2.plot(reads_c, versions_l, marker='x', linestyle='--', color='#1F77B4', linewidth=1.8, label="Leader (Up-to-date)")
    ax2.plot(reads_c, versions_f, marker='s', color='#D62728', linewidth=2.2, label="Follower (Lagged)")
    
    # Annotate violations where Follower version is behind Leader version
    for r, vl, vf in zip(reads_c, versions_l, versions_f):
        if vf < vl:
            ax2.annotate('VIOLATION!', xy=(r, vf), xytext=(r, vf - 1.2),
                         arrowprops=dict(facecolor='#D62728', shrink=0.08, width=1, headwidth=5),
                         fontsize=8, color='#D62728', fontweight='bold', ha='center')
                         
    ax2.set_title("B. Multi-Node Read (Leader -> Follower)\n[Monotonic Reads VIOLATION Demonstration]", fontsize=12, fontweight='bold', pad=10)
    ax2.set_xlabel("Sequential Read Count", fontsize=10)
    ax2.set_ylabel("Read Version (Version)", fontsize=10)
    ax2.set_yticks(range(0, 13))
    ax2.grid(True, linestyle=':', alpha=0.6)
    ax2.legend(loc="lower right")
    
    plt.suptitle("Experiment 2: Monotonic Reads Analysis", fontsize=14, fontweight='bold', y=0.98)
    plt.tight_layout()
    output_path = os.path.join(RESULTS_DIR, "monotonic_reads_plot.png")
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"Chart generated: {output_path}")

def plot_read_after_write():
    """
    Experiment 3: Read-After-Write Consistency - Leader vs Follower Lag Comparison
    """
    res = load_csv_data("raw_results.csv")
    if not res:
        return
    headers, rows = res
    
    trials = [int(row[0]) for row in rows]
    leader_lags = [float(row[3]) for row in rows]
    follower_lags = [float(row[5]) for row in rows]
    
    import numpy as np
    x = np.arange(len(trials))
    width = 0.35
    
    fig, ax = plt.subplots(figsize=(11, 5.5))
    rects1 = ax.bar(x - width/2, leader_lags, width, label='Leader Visibility Lag', color='#A2A2A2', alpha=0.8)
    rects2 = ax.bar(x + width/2, follower_lags, width, label='Follower Visibility Lag', color='#E67E22', alpha=0.85)
    
    ax.set_ylabel('Visibility Lag (ms - logarithmic scale)', fontsize=11)
    ax.set_title('Experiment 3: Read-After-Write Consistency — Leader vs Follower Lag', fontsize=13, fontweight='bold', pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels([f"Trial {t}" for t in trials])
    ax.set_yscale('log') # Logarithmic scale since Leader is ~0.1ms while Follower is ~10-100ms
    ax.grid(axis='y', linestyle='--', alpha=0.5, which="both")
    ax.legend(loc="upper right", frameon=True)
    
    # Annotate value labels
    for rect in rects2:
        height = rect.get_height()
        ax.annotate(f'{height:.1f}ms',
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=8, fontweight='bold', color='#B35300')
                    
    plt.tight_layout()
    output_path = os.path.join(RESULTS_DIR, "read_after_write_plot.png")
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"Chart generated: {output_path}")

def generate_all_plots():
    """
    Draws charts for all experiments and saves them to the results/ folder.
    """
    print("\n" + "=" * 60)
    print("MATPLOTLIB CHART VISUALIZATION STARTING")
    print("=" * 60)
    
    if not os.path.exists(RESULTS_DIR):
        print("ERROR: Error: Results folder not found! Please run the experiments first.")
        return
        
    plot_eventual_consistency()
    plot_monotonic_reads()
    plot_read_after_write()
    
    print("\nAll charts successfully generated and saved to the 'results/' folder.")
    print("They are ready to be included in your LaTeX report!")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    generate_all_plots()
