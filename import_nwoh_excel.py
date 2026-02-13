"""
Import NWOH historical data from Excel for dashboard display.
Parses Monthly PJM Wind Units Report and extracts daily/hourly data with PPA calculations.
"""
import pandas as pd
import json
from datetime import datetime
from collections import defaultdict

def import_nwoh_excel(excel_path, output_path="nwoh_historical_data.json"):
    """
    Import NWOH data from Excel and calculate PPA metrics.

    Args:
        excel_path: Path to the Monthly PJM Wind Units Report Excel file
        output_path: Where to save the JSON output

    Returns:
        Dictionary with daily, monthly, and annual aggregations
    """
    print(f"Importing NWOH data from: {excel_path}")

    # Read all relevant tabs
    daily_df = pd.read_excel(excel_path, sheet_name='Daily', header=1)
    hourly_df = pd.read_excel(excel_path, sheet_name='Hourly', header=1)
    ppa_5min_df = pd.read_excel(excel_path, sheet_name='PPA 5 Min Data', header=1)

    # Clean column names
    ppa_5min_df.columns = [
        'date', 'he', 'min_ending', 'hub_lmp', 'node_lmp', 'gen_mwh_hourly', 'gm_mwh',
        'floating_payment', 'fixed_payment', 'settlement', 'unnamed10',
        'floating_node', 'basis', 'min_floating', 'make_whole_hours'
    ] + [f'col{i}' for i in range(15, len(ppa_5min_df.columns))]

    # PPA price
    PPA_PRICE = 33.31

    # ===== Process PPA 5 Min Data for hub prices and PPA calculations =====
    print("Processing 5-minute PPA data...")

    # Filter valid data rows (where date is not NaN and has actual data)
    ppa_5min_df['date'] = pd.to_datetime(ppa_5min_df['date'], errors='coerce')
    ppa_data = ppa_5min_df[ppa_5min_df['date'].notna()].copy()

    # Convert to numeric
    for col in ['hub_lmp', 'node_lmp', 'gen_mwh_hourly', 'floating_payment', 'fixed_payment']:
        ppa_data[col] = pd.to_numeric(ppa_data[col], errors='coerce')

    # Gen MWh per 5-min interval (hourly value / 12)
    ppa_data['gen_mwh_5min'] = ppa_data['gen_mwh_hourly'] / 12

    # Products for weighted averages
    ppa_data['hub_product'] = ppa_data['gen_mwh_5min'] * ppa_data['hub_lmp']
    ppa_data['node_product'] = ppa_data['gen_mwh_5min'] * ppa_data['node_lmp']

    # Group by date for daily hub/basis calculations
    ppa_data['date_str'] = ppa_data['date'].dt.strftime('%Y-%m-%d')

    daily_ppa = ppa_data.groupby('date_str').agg({
        'gen_mwh_5min': 'sum',
        'hub_product': 'sum',
        'node_product': 'sum',
        'floating_payment': 'sum',
        'fixed_payment': 'sum',
    }).reset_index()

    # Calculate GWA prices and basis for each day
    daily_ppa['gwa_hub'] = daily_ppa['hub_product'] / daily_ppa['gen_mwh_5min']
    daily_ppa['gwa_node'] = daily_ppa['node_product'] / daily_ppa['gen_mwh_5min']
    daily_ppa['gwa_basis'] = daily_ppa['gwa_hub'] - daily_ppa['gwa_node']
    daily_ppa['net_ppa_settlement'] = daily_ppa['fixed_payment'] - daily_ppa['floating_payment']

    # Create lookup dict
    ppa_by_date = {}
    for _, row in daily_ppa.iterrows():
        ppa_by_date[row['date_str']] = {
            'gwa_hub': round(row['gwa_hub'], 2) if pd.notna(row['gwa_hub']) else None,
            'gwa_node': round(row['gwa_node'], 2) if pd.notna(row['gwa_node']) else None,
            'gwa_basis': round(row['gwa_basis'], 2) if pd.notna(row['gwa_basis']) else None,
            'floating_payment': round(row['floating_payment'], 2),
            'fixed_payment': round(row['fixed_payment'], 2),
            'net_ppa_settlement': round(row['net_ppa_settlement'], 2),
        }

    # ===== Process Daily DART Data =====
    print("Processing daily DART data...")

    daily_df['Date'] = pd.to_datetime(daily_df['Date'], format='%m/%d/%Y', errors='coerce')
    daily_df = daily_df[daily_df['Date'].notna()]

    # Build daily data dict
    daily_data = {}
    monthly_data = defaultdict(lambda: {
        'pnl': 0, 'volume': 0, 'da_mwh': 0, 'da_revenue': 0, 'rt_revenue': 0,
        'hub_product': 0, 'node_product': 0, 'da_lmp_product': 0, 'rt_lmp_product': 0,
        'count': 0
    })
    annual_data = defaultdict(lambda: {
        'pnl': 0, 'volume': 0, 'da_mwh': 0, 'da_revenue': 0, 'rt_revenue': 0,
        'hub_product': 0, 'node_product': 0, 'da_lmp_product': 0, 'rt_lmp_product': 0,
        'count': 0
    })

    for _, row in daily_df.iterrows():
        date_str = row['Date'].strftime('%Y-%m-%d')
        month_str = row['Date'].strftime('%Y-%m')
        year_str = row['Date'].strftime('%Y')

        gen_mwh = float(row['Gen MWh']) if pd.notna(row['Gen MWh']) else 0
        da_mwh = float(row['DAMWh']) if pd.notna(row['DAMWh']) else 0
        da_lmp = float(row['DALMP']) if pd.notna(row['DALMP']) else 0
        rt_lmp = float(row['RTLMP']) if pd.notna(row['RTLMP']) else 0
        da_revenue = float(row['Revenue DA']) if pd.notna(row['Revenue DA']) else 0
        rt_revenue = float(row['Revenue RT']) if pd.notna(row['Revenue RT']) else 0
        gross_revenue = float(row['Gross Revenue']) if pd.notna(row['Gross Revenue']) else 0

        # Get PPA data for this date
        ppa = ppa_by_date.get(date_str, {})
        gwa_hub = ppa.get('gwa_hub')
        gwa_node = ppa.get('gwa_node') or rt_lmp  # Fallback to RT LMP
        gwa_basis = ppa.get('gwa_basis')
        net_ppa = ppa.get('net_ppa_settlement', 0)

        # Total PnL = PJM Revenue + PPA Settlement
        # PJM Revenue = DA + RT (selling at node)
        # PPA Settlement = Gen × (PPA - Hub)
        total_pnl = gross_revenue + net_ppa

        daily_data[date_str] = {
            'pnl': round(total_pnl, 2),
            'volume': round(gen_mwh, 2),
            'da_mwh': round(da_mwh, 2),
            'da_revenue': round(da_revenue, 2),
            'rt_mwh': round(gen_mwh - da_mwh, 2),
            'rt_revenue': round(rt_revenue, 2),
            'rt_sales_revenue': round(rt_revenue, 2) if rt_revenue > 0 else 0,
            'rt_purchase_cost': round(abs(rt_revenue), 2) if rt_revenue < 0 else 0,
            'avg_da_price': round(da_lmp, 2),
            'avg_rt_price': round(rt_lmp, 2),
            'avg_hub_price': gwa_hub,
            'gwa_basis': gwa_basis,
            'realized_price': round(PPA_PRICE + (gwa_basis or 0), 2) if gwa_basis else None,
            'pjm_gross_revenue': round(gross_revenue, 2),
            'ppa_fixed_payment': ppa.get('fixed_payment'),
            'ppa_floating_payment': ppa.get('floating_payment'),
            'ppa_net_settlement': round(net_ppa, 2),
            'count': 1,
        }

        # Monthly aggregation
        monthly_data[month_str]['pnl'] += total_pnl
        monthly_data[month_str]['volume'] += gen_mwh
        monthly_data[month_str]['da_mwh'] += da_mwh
        monthly_data[month_str]['da_revenue'] += da_revenue
        monthly_data[month_str]['rt_revenue'] += rt_revenue
        monthly_data[month_str]['da_lmp_product'] += da_mwh * da_lmp
        monthly_data[month_str]['rt_lmp_product'] += gen_mwh * rt_lmp
        if gwa_hub and gen_mwh > 0:
            monthly_data[month_str]['hub_product'] += gen_mwh * gwa_hub
            monthly_data[month_str]['node_product'] += gen_mwh * gwa_node
        monthly_data[month_str]['count'] += 1

        # Annual aggregation
        annual_data[year_str]['pnl'] += total_pnl
        annual_data[year_str]['volume'] += gen_mwh
        annual_data[year_str]['da_mwh'] += da_mwh
        annual_data[year_str]['da_revenue'] += da_revenue
        annual_data[year_str]['rt_revenue'] += rt_revenue
        annual_data[year_str]['da_lmp_product'] += da_mwh * da_lmp
        annual_data[year_str]['rt_lmp_product'] += gen_mwh * rt_lmp
        if gwa_hub and gen_mwh > 0:
            annual_data[year_str]['hub_product'] += gen_mwh * gwa_hub
            annual_data[year_str]['node_product'] += gen_mwh * gwa_node
        annual_data[year_str]['count'] += 1

    # Calculate weighted averages for monthly/annual
    for month, d in monthly_data.items():
        if d['volume'] > 0:
            d['avg_da_price'] = round(d['da_lmp_product'] / d['da_mwh'], 2) if d['da_mwh'] > 0 else 0
            d['avg_rt_price'] = round(d['rt_lmp_product'] / d['volume'], 2)
            if d['hub_product'] > 0:
                d['avg_hub_price'] = round(d['hub_product'] / d['volume'], 2)
                d['gwa_basis'] = round((d['hub_product'] - d['node_product']) / d['volume'], 2)
                d['realized_price'] = round(PPA_PRICE + d['gwa_basis'], 2)
        d['pnl'] = round(d['pnl'], 2)
        d['volume'] = round(d['volume'], 2)
        d['da_revenue'] = round(d['da_revenue'], 2)
        d['rt_revenue'] = round(d['rt_revenue'], 2)
        del d['da_lmp_product']
        del d['rt_lmp_product']
        del d['hub_product']
        del d['node_product']

    for year, d in annual_data.items():
        if d['volume'] > 0:
            d['avg_da_price'] = round(d['da_lmp_product'] / d['da_mwh'], 2) if d['da_mwh'] > 0 else 0
            d['avg_rt_price'] = round(d['rt_lmp_product'] / d['volume'], 2)
            if d['hub_product'] > 0:
                d['avg_hub_price'] = round(d['hub_product'] / d['volume'], 2)
                d['gwa_basis'] = round((d['hub_product'] - d['node_product']) / d['volume'], 2)
                d['realized_price'] = round(PPA_PRICE + d['gwa_basis'], 2)
        d['pnl'] = round(d['pnl'], 2)
        d['volume'] = round(d['volume'], 2)
        d['da_revenue'] = round(d['da_revenue'], 2)
        d['rt_revenue'] = round(d['rt_revenue'], 2)
        del d['da_lmp_product']
        del d['rt_lmp_product']
        del d['hub_product']
        del d['node_product']

    # Build result
    result = {
        'asset': 'NWOH',
        'source': 'excel',
        'imported_at': datetime.now().isoformat(),
        'daily_pnl': daily_data,
        'monthly_pnl': dict(monthly_data),
        'annual_pnl': dict(annual_data),
        'total_pnl': round(sum(d['pnl'] for d in daily_data.values()), 2),
        'total_volume': round(sum(d['volume'] for d in daily_data.values()), 2),
    }

    # Save to JSON
    with open(output_path, 'w') as f:
        json.dump(result, f, indent=2)

    print(f"Saved to: {output_path}")
    print(f"Days imported: {len(daily_data)}")
    print(f"Total Volume: {result['total_volume']:,.2f} MWh")
    print(f"Total PnL: ${result['total_pnl']:,.2f}")

    # Show sample
    print("\nSample daily data:")
    for date in sorted(daily_data.keys())[:3]:
        d = daily_data[date]
        print(f"  {date}: Gen={d['volume']:.0f} MWh, PJM=${d['pjm_gross_revenue']:.0f}, PPA=${d['ppa_net_settlement']:.0f}, Total=${d['pnl']:.0f}")

    return result


if __name__ == "__main__":
    excel_path = r"C:\Users\TylerMartin\OneDrive - ArcLight Renewable Services\Desktop\Monthly PJM Wind Units Report_Northwest Ohio Wind_Jan2026.xlsx"
    result = import_nwoh_excel(excel_path)
