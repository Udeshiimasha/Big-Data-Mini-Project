# visualization/plot_traffic_report.py
"""
Matplotlib script to visualize Traffic Volume vs Time of Day from CSV reports.

Usage:
    python visualization/plot_traffic_report.py <path_to_csv_file>

Example:
    python visualization/plot_traffic_report.py airflow/reports/peak_traffic_20240101.csv
"""

import sys
import csv
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import os

def plot_traffic_report(csv_file_path):
    """
    Read CSV report and create visualization showing Traffic Volume vs Time of Day.
    """
    if not os.path.exists(csv_file_path):
        print(f"Error: File not found: {csv_file_path}")
        return
    
    # Read CSV data
    sensors_data = {}
    with open(csv_file_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sensor_id = row['sensor_id']
            peak_hour = int(row['peak_hour'])
            vehicle_count = int(row['vehicle_count'])
            
            if sensor_id not in sensors_data:
                sensors_data[sensor_id] = []
            sensors_data[sensor_id].append((peak_hour, vehicle_count))
    
    if not sensors_data:
        print("No data found in CSV file.")
        return
    
    # Create figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('Traffic Volume vs Time of Day - Peak Hours Analysis', fontsize=16, fontweight='bold')
    
    axes_flat = axes.flatten()
    
    # Extract date from filename if possible
    filename = os.path.basename(csv_file_path)
    date_str = filename.replace('peak_traffic_', '').replace('.csv', '')
    try:
        report_date = datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
    except:
        report_date = "Unknown Date"
    
    # Plot for each sensor
    for idx, (sensor_id, data) in enumerate(sensors_data.items()):
        if idx >= 4:
            break
        
        ax = axes_flat[idx]
        hours = [d[0] for d in data]
        volumes = [d[1] for d in data]
        
        # Create bar chart
        ax.bar(hours, volumes, color='steelblue', alpha=0.7, edgecolor='black')
        ax.set_xlabel('Hour of Day', fontsize=10)
        ax.set_ylabel('Vehicle Count', fontsize=10)
        ax.set_title(f'{sensor_id} - Peak Hour: {max(zip(volumes, hours), key=lambda x: x[0])[1]}:00', fontsize=11, fontweight='bold')
        ax.set_xlim(-0.5, 23.5)
        ax.set_xticks(range(0, 24, 2))
        ax.grid(axis='y', alpha=0.3, linestyle='--')
        
        # Highlight peak hour
        peak_hour = max(zip(volumes, hours), key=lambda x: x[0])[1]
        peak_volume = max(volumes)
        ax.bar(peak_hour, peak_volume, color='red', alpha=0.8, edgecolor='black', linewidth=2)
    
    # Hide unused subplots
    for idx in range(len(sensors_data), 4):
        axes_flat[idx].axis('off')
    
    plt.tight_layout()
    
    # Save figure
    output_file = csv_file_path.replace('.csv', '_visualization.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Visualization saved to: {output_file}")
    
    # Also create a combined line chart
    fig2, ax2 = plt.subplots(figsize=(12, 6))
    for sensor_id, data in sensors_data.items():
        hours = [d[0] for d in data]
        volumes = [d[1] for d in data]
        ax2.plot(hours, volumes, marker='o', label=sensor_id, linewidth=2, markersize=8)
    
    ax2.set_xlabel('Hour of Day', fontsize=12)
    ax2.set_ylabel('Vehicle Count', fontsize=12)
    ax2.set_title(f'Traffic Volume Comparison Across Junctions - {report_date}', fontsize=14, fontweight='bold')
    ax2.set_xlim(-0.5, 23.5)
    ax2.set_xticks(range(0, 24, 2))
    ax2.grid(alpha=0.3, linestyle='--')
    ax2.legend(loc='best', fontsize=10)
    
    plt.tight_layout()
    output_file2 = csv_file_path.replace('.csv', '_comparison.png')
    plt.savefig(output_file2, dpi=300, bbox_inches='tight')
    print(f"Comparison chart saved to: {output_file2}")
    
    plt.show()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python plot_traffic_report.py <path_to_csv_file>")
        print("\nExample:")
        print("  python visualization/plot_traffic_report.py airflow/reports/peak_traffic_20240101.csv")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    plot_traffic_report(csv_file)

