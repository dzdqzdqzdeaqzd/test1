import boto3
import json
import pandas as pd
import tkinter as tk
from tkinter import ttk, messagebox
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import threading
from PIL import Image, ImageTk
import os

# Configuration Scaleway
CONFIG = {
    "ACCESS_KEY": "SCWP8VGNTJ62QZTB6Y4A",
    "SECRET_KEY": "4e94d5cf-302b-45dd-86c1-32ac697f8bf4",
    "BUCKET_NAME": "capteur",
    "ENDPOINT_URL": "https://s3.fr-par.scw.cloud",
    "REFRESH_SECONDS": 10,
    "THEME": {
        "primary": "#2A2F3D",
        "secondary": "#3D4454",
        "accent": "#00C8AA",
        "text": "#FFFFFF",
        "warning": "#FF6B6B"
    },
    "TARIFS": {
        "pleine": 0.35,    # €/kWh
        "creuse": 0.18     # €/kWh
    },
    "VOLTAGE": 230         # Tension électrique en Volts
}

SENSORS = {
    "EM320_1": {"type": "temperature", "path": "sensor_data/em320_1", "unit": "°C", "max": 40},
    "EM320_2": {"type": "temperature", "path": "sensor_data/em320_2", "unit": "°C", "max": 40},
    "EM320_3": {"type": "temperature", "path": "sensor_data/em320_3", "unit": "°C", "max": 40},
    "EM320_4": {"type": "temperature", "path": "sensor_data/em320_4", "unit": "°C", "max": 40},
    "EM320_5": {"type": "temperature", "path": "sensor_data/em320_5", "unit": "°C", "max": 40},
    "CT101_1": {"type": "current", "path": "sensor_data/ct101_1", "unit": "A", "max": 15},
    "CT101_2": {"type": "current", "path": "sensor_data/ct101_2", "unit": "A", "max": 15},
    "CT101_3": {"type": "current", "path": "sensor_data/ct101_3", "unit": "A", "max": 15},
    "CT101_4": {"type": "current", "path": "sensor_data/ct101_4", "unit": "A", "max": 15},
    "CT101_5": {"type": "current", "path": "sensor_data/ct101_5", "unit": "A", "max": 15}
}

class EnhancedDataLoader:
    def __init__(self):
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=CONFIG["ACCESS_KEY"],
            aws_secret_access_key=CONFIG["SECRET_KEY"],
            endpoint_url=CONFIG["ENDPOINT_URL"]
        )
        self.data = pd.DataFrame()
        self.file_cache = {config["path"]: "" for sensor, config in SENSORS.items()}
        self.cost_data = self._initialize_cost_data()
        self._load_full_history()

    def _initialize_cost_data(self):
        return {sensor: {"total_cost": 0.0, "history": []} 
                for sensor in SENSORS if SENSORS[sensor]["type"] == "current"}

    def _load_full_history(self):
        """Charge tout l'historique au premier lancement"""
        for sensor, config in SENSORS.items():
            if config["type"] != "current":
                continue
            
            try:
                response = self.s3.list_objects_v2(Bucket=CONFIG["BUCKET_NAME"], Prefix=config["path"])
                if 'Contents' not in response:
                    continue
                
                prev_timestamp = None
                for obj in sorted(response['Contents'], key=lambda x: x['LastModified']):
                    if obj['Key'].endswith('.json'):
                        record = self._process_file(obj['Key'])
                        if record and 'current' in record:
                            current = record['current']
                            timestamp = record['timestamp']
                            
                            if prev_timestamp:
                                self._update_cost(sensor, current, prev_timestamp, timestamp)
                            
                            prev_timestamp = timestamp
                            self.cost_data[sensor]["history"].append({
                                "timestamp": timestamp,
                                "current": current,
                                "cost": self.cost_data[sensor]["total_cost"]
                            })
            
            except Exception as e:
                print(f"Erreur historique {sensor}: {str(e)}")

    def _update_cost(self, sensor, current, start_ts, end_ts):
        delta_h = (end_ts - start_ts).total_seconds() / 3600
        total_cost = 0.0
        current_ts = start_ts
        
        while current_ts < end_ts:
            next_ts = min(current_ts + timedelta(minutes=15), end_ts)
            partial_delta = (next_ts - current_ts).total_seconds() / 3600
            
            hour = current_ts.hour
            tarif = CONFIG["TARIFS"]["pleine"] if (8 <= hour < 13) or (18 <= hour < 21) else CONFIG["TARIFS"]["creuse"]
            
            puissance_kw = (current * CONFIG["VOLTAGE"]) / 1000
            total_cost += puissance_kw * partial_delta * tarif
            current_ts = next_ts
        
        self.cost_data[sensor]["total_cost"] += total_cost

    def _process_file(self, key):
        try:
            content = self.s3.get_object(
                Bucket=CONFIG["BUCKET_NAME"], 
                Key=key
            )['Body'].read().decode('utf-8')
            record = json.loads(content)
            record['timestamp'] = pd.to_datetime(record['timestamp'], utc=True)
            return record
        except Exception as e:
            print(f"Erreur fichier {key}: {str(e)}")
            return None

    def load_incremental_data(self):
        new_records = []
        for sensor, config in SENSORS.items():
            try:
                response = self.s3.list_objects_v2(
                    Bucket=CONFIG["BUCKET_NAME"],
                    Prefix=config["path"],
                    StartAfter=self.file_cache[config["path"]]
                )
                
                if 'Contents' in response:
                    for obj in response['Contents']:
                        if obj['Key'] > self.file_cache[config["path"]]:
                            record = self._process_file(obj['Key'])
                            if record:
                                record['sensor'] = sensor
                                new_records.append(record)
                                self.file_cache[config["path"]] = obj['Key']
                                
                                if config["type"] == "current" and 'current' in record:
                                    self._update_cost(sensor, record['current'], 
                                                    self.cost_data[sensor]["history"][-1]["timestamp"] if self.cost_data[sensor]["history"] else record['timestamp'],
                                                    record['timestamp'])
                                    self.cost_data[sensor]["history"].append({
                                        "timestamp": record['timestamp'],
                                        "current": record['current'],
                                        "cost": self.cost_data[sensor]["total_cost"]
                                    })
            except Exception as e:
                print(f"Erreur {sensor}: {str(e)}")
        
        if new_records:
            new_df = pd.DataFrame(new_records)
            self.data = pd.concat([self.data, new_df]).sort_values('timestamp').drop_duplicates()
        
        return self.data

class ProDashboardPlus:
    def __init__(self, root):
        self.root = root
        self.root.title("SmartFactory Pro+ Dashboard")
        self.root.geometry("1600x900")
        self.root.minsize(1200, 800)
        self.root.configure(bg=CONFIG["THEME"]["primary"])
        
        self.loader = EnhancedDataLoader()
        self.setup_styles()
        self.create_layout()
        self.load_initial_data()
        self.setup_refresh()

    def setup_styles(self):
        style = ttk.Style()
        style.theme_use('alt')
        
        style.configure('.', 
                      background=CONFIG["THEME"]["primary"], 
                      foreground=CONFIG["THEME"]["text"],
                      font=('Helvetica', 9))
        
        style.configure('TFrame', background=CONFIG["THEME"]["primary"])
        style.configure('TNotebook', background=CONFIG["THEME"]["secondary"], borderwidth=0)
        style.configure('TNotebook.Tab', 
                       background=CONFIG["THEME"]["secondary"],
                       foreground=CONFIG["THEME"]["text"],
                       padding=[20, 5],
                       font=('Helvetica', 10, 'bold'))
        
        style.map('TNotebook.Tab', 
                 background=[('selected', CONFIG["THEME"]["accent"])],
                 foreground=[('selected', CONFIG["THEME"]["primary"])])
        
        style.configure('Accent.TButton', 
                       background=CONFIG["THEME"]["accent"],
                       foreground=CONFIG["THEME"]["primary"],
                       font=('Helvetica', 9, 'bold'))

    def create_layout(self):
        # Header
        header_frame = ttk.Frame(self.root)
        header_frame.pack(fill='x', padx=20, pady=10)
        
        # Logo
        try:
            self.logo_img = ImageTk.PhotoImage(Image.open('logo.png').resize((120,40)))
        except FileNotFoundError:
            self.logo_img = ImageTk.PhotoImage(Image.new('RGB', (120,40), color=CONFIG["THEME"]["primary"]))
        
        ttk.Label(header_frame, image=self.logo_img).pack(side='left')
        
        self.status_label = ttk.Label(header_frame, 
                                    text="Système opérationnel",
                                    foreground=CONFIG["THEME"]["accent"],
                                    font=('Helvetica', 10))
        self.status_label.pack(side='right')

        # Main content
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill='both', expand=True, padx=20, pady=10)

        # Left panel
        left_panel = ttk.Frame(main_frame, width=350)
        left_panel.pack(side='left', fill='y')

        self.create_quick_stats(left_panel)
        self.create_cost_summary(left_panel)
        self.create_controls(left_panel)

        # Right panel
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(side='right', fill='both', expand=True, padx=(20, 0))

        self.setup_tabs()

        # Footer
        footer_frame = ttk.Frame(self.root, height=30)
        footer_frame.pack(fill='x', side='bottom')
        self.update_label = ttk.Label(footer_frame, 
                                    text=f"Dernière mise à jour: {datetime.now().strftime('%H:%M:%S')}",
                                    foreground=CONFIG["THEME"]["text"])
        self.update_label.pack(side='right', padx=20)

    def create_quick_stats(self, parent):
        stats_frame = ttk.Frame(parent)
        stats_frame.pack(fill='x', pady=10)
        
        ttk.Label(stats_frame, 
                 text="STATISTIQUES TEMPS RÉEL",
                 font=('Helvetica', 9, 'bold'),
                 foreground=CONFIG["THEME"]["accent"]).pack(anchor='w')

        self.stats_labels = {}
        for sensor in SENSORS:
            frame = ttk.Frame(stats_frame)
            frame.pack(fill='x', pady=2)
            
            ttk.Label(frame, 
                     text=sensor,
                     width=15,
                     foreground=CONFIG["THEME"]["text"]).pack(side='left')
            
            self.stats_labels[sensor] = ttk.Label(frame, 
                                                 text="--",
                                                 foreground=CONFIG["THEME"]["accent"],
                                                 font=('Helvetica', 9, 'bold'))
            self.stats_labels[sensor].pack(side='right')

    def create_cost_summary(self, parent):
        cost_frame = ttk.Frame(parent)
        cost_frame.pack(fill='x', pady=10)
        
        ttk.Label(cost_frame, 
                 text="COÛTS CUMULÉS",
                 font=('Helvetica', 9, 'bold'),
                 foreground=CONFIG["THEME"]["accent"]).pack(anchor='w')

        self.cost_labels = {}
        for sensor in [s for s in SENSORS if SENSORS[s]["type"] == "current"]:
            frame = ttk.Frame(cost_frame)
            frame.pack(fill='x', pady=2)
            
            ttk.Label(frame, 
                     text=sensor,
                     width=15,
                     foreground=CONFIG["THEME"]["text"]).pack(side='left')
            
            self.cost_labels[sensor] = ttk.Label(frame, 
                                                text="0.00 €",
                                                foreground=CONFIG["THEME"]["warning"],
                                                font=('Helvetica', 9, 'bold'))
            self.cost_labels[sensor].pack(side='right')

    def create_controls(self, parent):
        controls_frame = ttk.Frame(parent)
        controls_frame.pack(fill='x', pady=20)
        
        ttk.Button(controls_frame, 
                  text="Exporter Températures",
                  command=lambda: self.export_data('temperature'),
                  style='Accent.TButton').pack(fill='x', pady=2)
        
        ttk.Button(controls_frame, 
                  text="Exporter Courants",
                  command=lambda: self.export_data('current'),
                  style='Accent.TButton').pack(fill='x', pady=2)
        
        ttk.Separator(controls_frame, orient='horizontal').pack(fill='x', pady=10)
        
        self.alert_label = ttk.Label(controls_frame,
                                    text="",
                                    foreground=CONFIG["THEME"]["warning"],
                                    wraplength=300)
        self.alert_label.pack()

    def export_data(self, data_type):
        try:
            filename = f"{data_type}_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            writer = pd.ExcelWriter(filename, engine='xlsxwriter')
            
            # Création d'une copie pour éviter les warnings
            filtered_data = self.loader.data[self.loader.data['sensor'].isin(
                [s for s in SENSORS if SENSORS[s]["type"] == data_type]
            )].copy()
            
            # Conversion des dates pour Excel
            filtered_data['timestamp'] = filtered_data['timestamp'].dt.tz_localize(None)
            
            for sensor in filtered_data['sensor'].unique():
                sensor_data = filtered_data[filtered_data['sensor'] == sensor]
                sensor_data[['timestamp', SENSORS[sensor]["type"]]].to_excel(
                    writer, 
                    sheet_name=sensor[:31],  # Limite Excel à 31 caractères
                    index=False
                )
            
            writer.close()
            messagebox.showinfo("Export réussi", f"Fichier {filename} généré avec succès!")
        except Exception as e:
            messagebox.showerror("Erreur export", f"Échec de l'export : {str(e)}")

    def setup_tabs(self):
        self.tabs = {}
        self.figures = {}
        
        for sensor in SENSORS:
            tab = ttk.Frame(self.notebook)
            self.notebook.add(tab, text=sensor)
            self.tabs[sensor] = tab

            fig = plt.Figure(figsize=(8, 4), dpi=100)
            fig.patch.set_facecolor(CONFIG["THEME"]["secondary"])
            ax = fig.add_subplot(111)
            ax.set_facecolor(CONFIG["THEME"]["primary"])
            
            ax.tick_params(colors=CONFIG["THEME"]["text"], which='both')
            for spine in ax.spines.values():
                spine.set_color(CONFIG["THEME"]["text"])
            ax.grid(color=CONFIG["THEME"]["text"], alpha=0.1)
            
            canvas = FigureCanvasTkAgg(fig, master=tab)
            canvas.get_tk_widget().pack(fill='both', expand=True)
            
            toolbar = NavigationToolbar2Tk(canvas, tab)
            toolbar.update()
            
            self.figures[sensor] = (fig, ax)

    def load_initial_data(self):
        self.status_label.config(text="Chargement initial...")
        threading.Thread(target=self._async_initial_load, daemon=True).start()

    def _async_initial_load(self):
        try:
            self.loader.load_incremental_data()
            self.root.after(0, self.initial_plot)
        except Exception as e:
            self.root.after(0, lambda: self.show_error(f"Erreur de connexion: {str(e)}"))

    def initial_plot(self):
        for sensor in SENSORS:
            data = self.loader.data[self.loader.data['sensor'] == sensor]
            if not data.empty:
                self._plot_data(sensor, data)
                self._update_stats(sensor, data.iloc[-1])
        self._update_costs()
        self.status_label.config(text="Système opérationnel")

    def _plot_data(self, sensor, data):
        fig, ax = self.figures[sensor]
        ax.clear()
        
        config = SENSORS[sensor]
        ax.plot(data['timestamp'], 
               data[config['type']], 
               color=CONFIG["THEME"]["accent"],
               linewidth=1.5,
               marker='o',
               markersize=4)
        
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d %b\n%H:%M'))
        ax.set_title(f"Historique {sensor}", color=CONFIG["THEME"]["text"], pad=20)
        ax.set_ylabel(config['unit'], color=CONFIG["THEME"]["text"])
        fig.canvas.draw_idle()

    def _update_stats(self, sensor, latest):
        config = SENSORS[sensor]
        value = f"{latest[config['type']]:.1f}{config['unit']}"
        self.stats_labels[sensor].config(text=value)
        
        if config['type'] == 'current' and latest['current'] > config['max']:
            self._trigger_alert(sensor, "Dépassement de seuil critique!")

    def _update_costs(self):
        for sensor in [s for s in SENSORS if SENSORS[s]["type"] == "current"]:
            total_cost = self.loader.cost_data[sensor]["total_cost"]
            self.cost_labels[sensor].config(text=f"{total_cost:.2f} €")

    def _trigger_alert(self, sensor, message):
        self.alert_label.config(text=f"⚠️ ALERTE {sensor}: {message}")
        self.root.bell()
        self.root.after(5000, lambda: self.alert_label.config(text=""))

    def setup_refresh(self):
        self.root.after(CONFIG["REFRESH_SECONDS"] * 1000, self.refresh_data)

    def refresh_data(self):
        threading.Thread(target=self._async_refresh, daemon=True).start()

    def _async_refresh(self):
        try:
            old_size = len(self.loader.data)
            new_data = self.loader.load_incremental_data()
            self.root.after(0, lambda: self._handle_new_data(old_size, new_data))
        except Exception as e:
            self.root.after(0, lambda: self.show_error(str(e)))

    def _handle_new_data(self, old_size, new_data):
        if len(new_data) > old_size:
            for sensor in SENSORS:
                sensor_data = new_data[new_data['sensor'] == sensor]
                if not sensor_data.empty:
                    self._plot_data(sensor, sensor_data)
                    self._update_stats(sensor, sensor_data.iloc[-1])
            self._update_costs()
        
        self.update_label.config(text=f"Dernière mise à jour: {datetime.now().strftime('%H:%M:%S')}")
        self.root.after(0, self.setup_refresh)

    def show_error(self, message):
        messagebox.showerror("Erreur système", message)
        self.status_label.config(text="Erreur - Voir les logs", foreground=CONFIG["THEME"]["warning"])

if __name__ == "__main__":
    root = tk.Tk()
    app = ProDashboardPlus(root)
    root.mainloop()
