"""
Load Data Curator — Realistic Synthetic Residential Load Dataset for North India
=================================================================================
Generates hourly electricity load profiles for 75 homes (15 per city) across
5 Northern Indian cities using NASA POWER weather data + calibrated behavioral
synthesis. Parameters are benchmarked against:
  - CEA/CERC residential consumption data (180-220 kWh/month Delhi middle-class)
  - Prayas Energy Group hourly load studies
  - TERI residential sector analysis
  - Published smart-meter research (Bath/ResearchGate)
"""

import os
import requests
import pandas as pd
import numpy as np
import time
from io import StringIO
from datetime import date, timedelta

# ──────────────────────────────────────────────────────────
#  CONFIGURATION
# ──────────────────────────────────────────────────────────

CITIES = {
    "Delhi":      {"lat": 28.6139, "lon": 77.2090, "elev": 216, "temp_floor": 1.5},
    "Noida":      {"lat": 28.5355, "lon": 77.3910, "elev": 200, "temp_floor": 1.5},
    "Gurugram":   {"lat": 28.4595, "lon": 77.0266, "elev": 217, "temp_floor": 1.5},
    "Chandigarh": {"lat": 30.7333, "lon": 76.7794, "elev": 321, "temp_floor": 0.5},
    "Dehradun":   {"lat": 30.3165, "lon": 78.0322, "elev": 640, "temp_floor": -0.5},
}

# Deterministic per-city seed offset so same home_index across cities differ
CITY_SEED_OFFSET = {
    "Delhi": 0, "Noida": 10000, "Gurugram": 20000,
    "Chandigarh": 30000, "Dehradun": 40000,
}

YEARS = [2019, 2020, 2021, 2022, 2023]
HOMES_PER_CITY = 15

BASE_URL = "https://power.larc.nasa.gov/api/temporal/hourly/point"
OUTPUT_DIR = "forecasting/data/load"
RAW_DIR = os.path.join("forecasting", "data", "raw", "weather")


# ──────────────────────────────────────────────────────────
#  INDIAN HOLIDAY CALENDAR (2019–2023)
# ──────────────────────────────────────────────────────────

def _build_holiday_calendar():
    """
    Build a lookup dict of (year, month, day) -> load multiplier for
    major Indian festivals and national holidays. Lunar-calendar festivals
    use actual historical dates. Buffer days around major festivals
    capture multi-day celebrations (e.g. Diwali week).
    """
    holidays = {}

    def add(year, month, day, multiplier, buffer_days=0):
        for offset in range(-buffer_days, buffer_days + 1):
            try:
                dt = date(year, month, day) + timedelta(days=offset)
                # Buffer days get a reduced multiplier
                adj = multiplier if offset == 0 else 1 + (multiplier - 1) * 0.5
                key = f"{dt.year}_{dt.month}_{dt.day}"
                holidays[key] = max(holidays.get(key, 1.0), adj)
            except ValueError:
                pass

    years = YEARS

    # ── Fixed National Holidays ──
    for y in years:
        add(y,  1,  1, 1.15)        # New Year
        add(y,  1, 13, 1.15)        # Lohri (North India)
        add(y,  1, 14, 1.12)        # Makar Sankranti
        add(y,  1, 26, 1.15)        # Republic Day
        add(y,  8, 15, 1.15)        # Independence Day
        add(y, 10,  2, 1.10)        # Gandhi Jayanti
        add(y, 11,  1, 1.08)        # All Saints / Haryana Day
        add(y, 12, 25, 1.15)        # Christmas

    # ── Diwali (±2 days for extended celebrations + deep cleaning / lighting) ──
    diwali = {2019: (10, 27), 2020: (11, 14), 2021: (11, 4),
              2022: (10, 24), 2023: (11, 12)}
    for y, (m, d) in diwali.items():
        add(y, m, d, 1.35, buffer_days=2)

    # ── Holi (day of + day before with Holika Dahan) ──
    holi = {2019: (3, 21), 2020: (3, 10), 2021: (3, 29),
            2022: (3, 18), 2023: (3, 8)}
    for y, (m, d) in holi.items():
        add(y, m, d, 1.25, buffer_days=1)

    # ── Dussehra (end of Navratri — 1 day buffer) ──
    dussehra = {2019: (10, 8), 2020: (10, 25), 2021: (10, 15),
                2022: (10, 5), 2023: (10, 24)}
    for y, (m, d) in dussehra.items():
        add(y, m, d, 1.20, buffer_days=1)

    # ── Eid ul-Fitr (±1 day) ──
    eid_fitr = {2019: (6, 5), 2020: (5, 24), 2021: (5, 13),
                2022: (5, 3), 2023: (4, 22)}
    for y, (m, d) in eid_fitr.items():
        add(y, m, d, 1.20, buffer_days=1)

    # ── Eid ul-Adha (±1 day) ──
    eid_adha = {2019: (8, 12), 2020: (8, 1), 2021: (7, 21),
                2022: (7, 10), 2023: (6, 29)}
    for y, (m, d) in eid_adha.items():
        add(y, m, d, 1.18, buffer_days=1)

    # ── Raksha Bandhan ──
    raksha = {2019: (8, 15), 2020: (8, 3), 2021: (8, 22),
              2022: (8, 11), 2023: (8, 30)}
    for y, (m, d) in raksha.items():
        add(y, m, d, 1.12)

    # ── Karva Chauth (North India specific — evening celebrations, lighting) ──
    karva = {2019: (10, 17), 2020: (11, 4), 2021: (10, 24),
             2022: (10, 13), 2023: (11, 1)}
    for y, (m, d) in karva.items():
        add(y, m, d, 1.12)

    return holidays


HOLIDAY_CALENDAR = _build_holiday_calendar()


# ──────────────────────────────────────────────────────────
#  NASA POWER API FETCH
# ──────────────────────────────────────────────────────────

def fetch_nasa_weather(city_name, lat, lon, year):
    """Fetch hourly temperature and humidity data from NASA POWER API."""
    print(f"  Fetching NASA weather for {city_name} in {year}...")

    params = {
        "parameters": "T2M,RH2M",
        "community": "RE",
        "longitude": lon,
        "latitude": lat,
        "start": f"{year}0101",
        "end": f"{year}1231",
        "format": "CSV",
        "time-standard": "LST",
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=60)
        response.raise_for_status()
        content = response.text
        csv_start = content.find("YEAR,MO,DY,HR")
        if csv_start == -1:
            print(f"    Error: CSV header not found for {city_name} {year}")
            return None
        return pd.read_csv(StringIO(content[csv_start:]))
    except Exception as e:
        print(f"    Failed: {city_name} {year}: {e}")
        return None


# ──────────────────────────────────────────────────────────
#  LOAD SYNTHESIZER  (Calibrated for Indian Residential)
# ──────────────────────────────────────────────────────────

def synthesize_load(df, city_name, home_index, temp_floor):
    """
    Generate a realistic synthetic hourly load profile for a single
    North Indian household.

    Calibration targets (Delhi middle-class benchmark):
      - Monthly consumption: 180–250 kWh/month
      - Summer/Winter ratio: 1.4–1.7×
      - Peak evening load:   0.35–0.65 kW
      - Overnight minimum:   0.03–0.10 kW
      - Max instantaneous:   ≤ 3.5 kW (3-AC upper-middle-class extreme)
    """
    df = df.copy()

    # ── Timestamp creation ──
    df['timestamp'] = pd.to_datetime({
        'year': df['YEAR'], 'month': df['MO'],
        'day': df['DY'], 'hour': df['HR'],
    })

    # ── Clip satellite temperature artifacts ──
    # NASA POWER uses reanalysis data which can undershoot ground-level
    # minimums for these cities; clip to realistic recorded lows
    df['T2M'] = df['T2M'].clip(lower=temp_floor)

    hour        = df['HR']
    month       = df['MO']
    day_of_week = df['timestamp'].dt.dayofweek
    temp        = df['T2M']
    humidity    = df['RH2M']

    # ── Home Individualization (deterministic per city×home) ──
    seed = home_index * 1337 + CITY_SEED_OFFSET.get(city_name, 0)
    rng = np.random.default_rng(seed=seed)

    # Shift peak hours (early risers vs late sleepers)
    morning_peak_hour  = rng.uniform(6.5, 9.0)
    evening_peak_hour  = rng.uniform(18.5, 21.5)

    # Baseline standby power (fridge, router, chargers, standby electronics)
    base_load = rng.uniform(0.03, 0.10)       # kW

    # Peak magnitudes — calibrated for Indian residential
    morning_mag = rng.uniform(0.08, 0.25)      # Geyser, cooking, getting ready
    evening_mag = rng.uniform(0.15, 0.42)      # Lighting, TV, cooking, fans

    # Climate sensitivity — calibrated
    ac_threshold     = rng.uniform(26, 30)     # °C where AC kicks in
    heating_threshold = rng.uniform(12, 18)    # °C where heater kicks in
    ac_sensitivity   = rng.uniform(0.010, 0.032)   # AC load coefficient
    heat_sensitivity = rng.uniform(0.005, 0.018)   # Heater load coefficient
    ac_saturation    = rng.uniform(1.0, 2.0)       # Max AC draw (kW)

    # Late-night AC (sleeping with AC on during Delhi summers)
    night_ac_kw = rng.uniform(0.10, 0.35)

    # ── 1. Base Daily Profile (Gaussian peaks) ──
    morning_peak = morning_mag * np.exp(
        -((hour - morning_peak_hour) ** 2) / (2 * 1.5 ** 2)
    )
    evening_peak = evening_mag * np.exp(
        -((hour - evening_peak_hour) ** 2) / (2 * 2.5 ** 2)
    )
    total_base = base_load + morning_peak + evening_peak

    # ── 2. Weather-Driven Cooling Load (AC) ──
    cooling_load = np.where(
        temp > ac_threshold,
        ac_sensitivity * (temp - ac_threshold) ** 1.2,
        0.0,
    )
    # Humidity amplification (high humidity → AC works harder)
    humidity_factor = (1 + (humidity - 50) / 150).clip(0.85, 1.3)
    cooling_load = (cooling_load * humidity_factor).clip(0, ac_saturation)

    # ── 3. Weather-Driven Heating Load (room heater / geyser) ──
    heating_load = np.where(
        temp < heating_threshold,
        heat_sensitivity * (heating_threshold - temp) ** 1.2,
        0.0,
    )

    # ── 4. Late-Night AC Plateau (summer sleep pattern, 9 PM – 2 AM) ──
    # Research shows Delhi AC demand peaks 9 PM–2 AM (people sleeping with AC)
    is_night = (hour >= 21) | (hour <= 2)
    is_warm_night = temp > 25
    night_ac_load = np.where(
        is_night & is_warm_night,
        night_ac_kw * (1 - np.exp(-0.3 * (temp - 25))),
        0.0,
    )

    # ── 5. Weekend Effect (people home all day) ──
    weekend_mult = np.where(day_of_week >= 5, 1.15, 1.0)

    # ── 6. Seasonal Factor (subtle — daylight hours, lifestyle shifts) ──
    seasonal_factor = 1 + 0.07 * np.cos(2 * np.pi * (month - 6) / 12)

    # ── 7. Holiday Effect (vectorized lookup) ──
    date_keys = (
        df['YEAR'].astype(int).astype(str) + '_' +
        df['MO'].astype(int).astype(str) + '_' +
        df['DY'].astype(int).astype(str)
    )
    holiday_mult = date_keys.map(HOLIDAY_CALENDAR).fillna(1.0).values

    # ── Combined Load ──
    load = (
        (total_base + cooling_load + heating_load + night_ac_load)
        * weekend_mult
        * seasonal_factor
        * holiday_mult
    )

    # ── 8. Stochastic Noise (appliance switching, ~10% hourly variation) ──
    noise = rng.normal(1.0, 0.10, size=len(load))
    load *= noise

    # ── 9. Realistic Clipping ──
    # Min: standby electronics (~30W)
    # Max: 3.5 kW (3-AC upper-middle-class home with all appliances)
    load = load.clip(lower=0.03, upper=3.5)

    df['load_kw'] = load
    df['home_id'] = f"{city_name}_{home_index:02d}"
    df.rename(columns={'T2M': 'temp_air', 'RH2M': 'humidity'}, inplace=True)

    return df


# ──────────────────────────────────────────────────────────
#  MAIN PIPELINE
# ──────────────────────────────────────────────────────────

def main():
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_city_data = []

    print(f"{'='*60}")
    print(f"  Load Data Curator — {len(CITIES)} cities × {HOMES_PER_CITY} homes")
    print(f"{'='*60}")

    for city_name, info in CITIES.items():
        city_weather_dfs = []

        for year in YEARS:
            cache_file = os.path.join(RAW_DIR, f"{city_name}_{year}_weather.csv")

            if os.path.exists(cache_file):
                print(f"  [cache] {city_name} {year}")
                df_year = pd.read_csv(cache_file)
            else:
                df_year = fetch_nasa_weather(
                    city_name, info['lat'], info['lon'], year
                )
                if df_year is not None:
                    df_year.to_csv(cache_file, index=False)
                    time.sleep(1)  # Rate-limit NASA API

            if df_year is not None:
                city_weather_dfs.append(df_year)

        if not city_weather_dfs:
            print(f"  [SKIP] No weather data for {city_name}")
            continue

        weather_combined = pd.concat(city_weather_dfs, ignore_index=True)

        print(f"  Synthesizing {HOMES_PER_CITY} homes for {city_name}...")
        for i in range(HOMES_PER_CITY):
            home_df = synthesize_load(
                weather_combined, city_name, i, info['temp_floor']
            )
            home_df['city'] = city_name
            home_df['lat']  = info['lat']
            home_df['lon']  = info['lon']
            all_city_data.append(home_df)

    # ── Merge & Feature Engineering ──
    print("\nMerging into final dataset...")
    final_df = pd.concat(all_city_data, ignore_index=True)

    final_df['hour']        = final_df['timestamp'].dt.hour
    final_df['month']       = final_df['timestamp'].dt.month
    final_df['day_of_week'] = final_df['timestamp'].dt.dayofweek
    final_df['is_weekend']  = (final_df['day_of_week'] >= 5).astype(int)

    # Lag features (grouped by home to prevent cross-home leakage)
    print("  Computing lag features...")
    final_df.sort_values(['home_id', 'timestamp'], inplace=True)
    final_df['load_lag_1h']  = final_df.groupby('home_id')['load_kw'].shift(1)
    final_df['load_lag_24h'] = final_df.groupby('home_id')['load_kw'].shift(24)

    final_df.dropna(inplace=True)

    cols = [
        'timestamp', 'home_id', 'city', 'lat', 'lon',
        'temp_air', 'humidity',
        'hour', 'month', 'day_of_week', 'is_weekend',
        'load_lag_1h', 'load_lag_24h', 'load_kw',
    ]
    final_df = final_df[cols]

    output_path = os.path.join(OUTPUT_DIR, "load_data_north_india.csv")
    final_df.to_csv(output_path, index=False)

    # ── Summary Stats ──
    print(f"\n{'='*60}")
    print(f"  SUCCESS — {output_path}")
    print(f"{'='*60}")
    print(f"  Total rows:   {len(final_df):,}")
    print(f"  Unique homes: {final_df['home_id'].nunique()}")
    print(f"  Date range:   {final_df['timestamp'].min()} → {final_df['timestamp'].max()}")
    print(f"  Mean load:    {final_df['load_kw'].mean():.3f} kW")
    print(f"  Max load:     {final_df['load_kw'].max():.3f} kW")

    # Per-city monthly kWh
    print(f"\n  Per-city avg monthly consumption:")
    for city in sorted(final_df['city'].unique()):
        cdf = final_df[final_df['city'] == city]
        homes = cdf['home_id'].unique()
        monthly_list = []
        for h in homes[:5]:
            hdf = cdf[cdf['home_id'] == h]
            total_kwh = hdf['load_kw'].sum()
            months = len(hdf) / (30.44 * 24)
            monthly_list.append(total_kwh / months if months > 0 else 0)
        print(f"    {city:15s}: ~{np.mean(monthly_list):.0f} kWh/month")


if __name__ == "__main__":
    main()
