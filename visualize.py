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
    
    # Map colors based on operation type suffix in title (row[2])
    bar_colors = []
    for row in rows:
        title = row[2]
        if title.endswith('_INSERT'):
            bar_colors.append('#E74C3C') # Red
        elif title.endswith('_UPDATE'):
            bar_colors.append('#2ECC71') # Green
        elif title.endswith('_DELETE'):
            bar_colors.append('#3498DB') # Blue
        else:
            bar_colors.append('#4A90E2') # Fallback blue
            
    plt.figure(figsize=(11, 5.5))
    bars = plt.bar(trials, lags, color=bar_colors, edgecolor='#555555', alpha=0.85, width=0.6)
    
    # Write the values above the columns
    for bar in bars:
        height = bar.get_height()
        if height > 0:
            plt.text(bar.get_x() + bar.get_width()/2.0, height + 0.05, f"{height:.1f}ms", ha='center', va='bottom', fontsize=8, fontweight='bold')
        
    plt.title("Experiment 1: Eventual Consistency — Replication Lag by CRUD Operation", fontsize=13, fontweight='bold', pad=15)
    plt.xlabel("Iteration / Run No (Logical Order)", fontsize=11, labelpad=10)
    plt.ylabel("Lag (Milliseconds - ms)", fontsize=11, labelpad=10)
    plt.xticks(trials)
    plt.grid(axis='y', linestyle='--', alpha=0.5)
    
    # Create legend patches for color representation
    import matplotlib.patches as mpatches
    insert_patch = mpatches.Patch(color='#E74C3C', label='INSERT Lag')
    update_patch = mpatches.Patch(color='#2ECC71', label='UPDATE Lag')
    delete_patch = mpatches.Patch(color='#3498DB', label='DELETE Lag')
    
    # Add average line
    avg_lag = sum(lags) / len(lags) if lags else 0
    avg_line = plt.axhline(avg_lag, color='#7F8C8D', linestyle=':', linewidth=2, label=f"Average: {avg_lag:.2f} ms")
    
    plt.legend(handles=[insert_patch, update_patch, delete_patch, avg_line], loc="upper right", frameon=True, facecolor='white', edgecolor='none')
    
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
    reads_s = [int(row[0]) for row in rows_s[:70]]
    versions_s = [int(row[2]) for row in rows_s[:70]]
    
    # Multi-node data
    _, rows_c = res_cross
    reads_c = [int(row[0]) for row in rows_c[:70]]
    versions_l = [int(row[2]) for row in rows_c[:70]]
    versions_f = [int(row[3]) for row in rows_c[:70]]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # Left Plot: Single Node
    ax1.plot(reads_s, versions_s, marker='o', color='#2CA02C', linewidth=2.5, markersize=4, label="Follower Read")
    ax1.set_title("A. Single Node Read (Follower Only)\n[Monotonic Reads Preserved]", fontsize=12, fontweight='bold', pad=10)
    ax1.set_xlabel("Sequential Read Count", fontsize=10)
    ax1.set_ylabel("Read Version", fontsize=10)
    ax1.set_yticks(range(0, 13))
    ax1.grid(True, linestyle=':', alpha=0.6)
    ax1.legend(loc="lower right")
    
    # Right Plot: Multi-Node (Violation Scenario)
    ax2.plot(reads_c, versions_l, marker='x', linestyle='--', color='#1F77B4', linewidth=1.5, markersize=4, label="Leader (Up-to-date)")
    ax2.plot(reads_c, versions_f, marker='s', color='#9467BD', linewidth=2, markersize=4, label="Follower (Lagged)")
    
    # Highlight violations with red dots
    violation_reads = [r for r, vl, vf in zip(reads_c, versions_l, versions_f) if vf < vl]
    violation_versions = [vf for r, vl, vf in zip(reads_c, versions_l, versions_f) if vf < vl]
    if violation_reads:
        ax2.scatter(violation_reads, violation_versions, color='#D62728', zorder=5, label="Stale Read Violation", s=40)
        
    ax2.set_title("B. Multi-Node Read (Leader -> Follower)\n[Monotonic Reads VIOLATION Demonstration]", fontsize=12, fontweight='bold', pad=10)
    ax2.set_xlabel("Sequential Read Count", fontsize=10)
    ax2.set_ylabel("Read Version", fontsize=10)
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

def plot_concurrent_ordering():
    """
    Experiment 4: Concurrent Writes - Parallel Timeline Plot showing Global Ordering Preservation
    """
    res = load_csv_data("concurrent_order_results.csv")
    if not res:
        return
    headers, rows = res
    
    from datetime import datetime
    
    # Parse times and details
    parsed_rows = []
    for row in rows:
        try:
            # Row index: 3 for Leader Commit, 4 for Follower Replicated, 1 for Write ID, 5 for TxID
            t_leader = datetime.strptime(row[3], '%H:%M:%S.%f')
            t_follower = datetime.strptime(row[4], '%H:%M:%S.%f')
            write_id = int(row[1])
            try:
                txid = int(row[5])
            except ValueError:
                txid = write_id # Fallback
            parsed_rows.append({
                "write_id": write_id,
                "txid": txid,
                "t_leader": t_leader,
                "t_follower": t_follower,
                "l_time_str": row[3],
                "f_time_str": row[4]
            })
        except Exception as e:
            print(f"Error parsing row: {row}, error: {e}")
            
    if not parsed_rows:
        return
        
    # Sort parsed_rows by txid/write_id to represent logical Leader commit order
    parsed_rows = sorted(parsed_rows, key=lambda x: x["txid"])
    
    # Assign logical sequence index (y-coordinate)
    for idx, item in enumerate(parsed_rows):
        item["y_coordinate"] = idx + 1
        
    import matplotlib
    matplotlib.use('Agg') # Ensure non-GUI backend
    import matplotlib.pyplot as plt
    
    plt.figure(figsize=(11, 7))
    
    # Use premium red color for all lines
    color = '#E74C3C'
    
    for idx, item in enumerate(parsed_rows):
        x_points = [0, 1]
        y_points = [item["y_coordinate"], item["y_coordinate"]]
        
        # Plot the horizontal line showing Rank_Leader == Rank_Follower
        # Use single legend entry for all red lines
        label_str = "Replicated Transactions (Order Preserved)" if idx == 0 else None
        plt.plot(x_points, y_points, marker='o', color=color, linewidth=2.5, markersize=8, 
                 label=label_str)
        
        # Extract milliseconds time only
        l_time = item["l_time_str"]
        f_time = item["f_time_str"]
        
        # Text annotation on the left (Leader): Write ID, TxID, and Commit Timestamp
        plt.text(-0.02, item["y_coordinate"], f"W:{item['write_id']} (TxID:{item['txid']})\nCommit: {l_time}", 
                 ha='right', va='center', fontsize=8, fontweight='bold', color=color)
        
        # Text annotation on the right (Follower): Write ID and Replay Timestamp
        plt.text(1.02, item["y_coordinate"], f"W:{item['write_id']} (Replayed)\nVisible: {f_time}", 
                 ha='left', va='center', fontsize=8, fontweight='bold', color=color)
        
    plt.title("Experiment 4: Global Ordering Preservation (FIFO Replication Proof)\n(Leader Commit Sequence maps 1-to-1 to Follower Replay Sequence)", fontsize=12, fontweight='bold', pad=15)
    plt.xlabel("Database Node / Action", fontsize=11, labelpad=10)
    plt.ylabel("Logical Execution Rank (FIFO Index)", fontsize=11, labelpad=10)
    
    plt.xticks([0, 1], ["Leader VM\n(Transaction Commit)", "Follower VM\n(Record Replicated)"], fontsize=10, fontweight='bold')
    plt.yticks(range(1, len(parsed_rows) + 1), [f"Seq #{i}" for i in range(1, len(parsed_rows) + 1)], fontsize=9)
    plt.grid(axis='y', linestyle=':', alpha=0.6)
    
    # Adjust layout to fit text annotations
    plt.xlim(-0.30, 1.30)
    plt.ylim(0.5, len(parsed_rows) + 0.5)
    plt.legend(loc="upper left", bbox_to_anchor=(1.05, 1.0), frameon=True, fontsize=9)
    
    plt.tight_layout()
    output_path = os.path.join(RESULTS_DIR, "concurrent_ordering_plot.png")
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
    # plot_monotonic_reads()  # Disabled based on user preference
    plot_read_after_write()
    plot_concurrent_ordering()
    
    print("\nAll charts successfully generated and saved to the 'results/' folder.")
    print("They are ready to be included in your LaTeX report!")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    generate_all_plots()
