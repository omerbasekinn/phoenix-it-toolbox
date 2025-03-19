import gspread
import pandas as pd
from datetime import datetime, timedelta
import os
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk
from tkcalendar import DateEntry

pd.options.mode.chained_assignment = None

def load_data():
    log_text.insert(tk.END, "🔌 Connecting to Google Sheets...\n")
    log_text.update()

    credential_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'credentials.json')
    gc = gspread.oauth(credentials_filename=credential_path)
    spreadsheet = gc.open("CX IT")
    worksheet = spreadsheet.sheet1
    records = worksheet.get_all_records()
    df = pd.DataFrame(records)

    log_text.insert(tk.END, "📥 Pulling data...\n")
    log_text.update()

    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    today = pd.Timestamp.today().normalize()
    eight_days_ago = today - pd.Timedelta(days=120)
    df = df[df['Date'] >= eight_days_ago]

    required_columns = [
        'Date', 'Customer', 'Region', 'Time Log', 'Assignee1',
        'FixTime', 'Tier2', 'DailyDiary', 'Results Not On Time', 'Platform', 'Jira Time Log'
    ]
    existing_columns = [col for col in required_columns if col in df.columns]
    return df[existing_columns]

def convert_time_format(t):
    if pd.isna(t): return ""
    try:
        h, m, s = map(int, str(t).split(":"))
        return f"{h}h {m}m" if h else f"0h {m}m"
    except Exception:
        return ""

def process_and_save(start_date, end_date, rows_per_file, output_dir, df, log_box, selected_assignee):
    try:
        mask = (df['Date'] >= pd.to_datetime(start_date)) & (df['Date'] <= pd.to_datetime(end_date))
        filtered = df.loc[mask].copy()

        if selected_assignee and selected_assignee != "All":
            filtered = filtered[filtered['Assignee1'] == selected_assignee]

        if filtered.empty:
            log_box.insert(tk.END, "⚠️ No data found for the selected filter.\n")
            return

        output_df = pd.DataFrame()
        output_df['Key'] = filtered['Jira Time Log']
        output_df['Time Spent (h)'] = filtered['Time Log'].apply(convert_time_format)
        output_df['Display Name'] = filtered['Assignee1']
        output_df['Date Started'] = pd.to_datetime(filtered['Date']).dt.date

        today_str = datetime.today().strftime("%Y-%m-%d")
        chunks = [output_df[i:i + rows_per_file] for i in range(0, output_df.shape[0], rows_per_file)]
        for idx, chunk in enumerate(chunks):
            filename = os.path.join(output_dir, f"{today_str}_CX_IT_Formatted_Part_{idx + 1}.csv")
            chunk.to_csv(filename, index=False, encoding="utf-8-sig")
            log_box.insert(tk.END, f"✅ Saved: {filename}\n")

        log_box.insert(tk.END, "🎉 All files generated successfully.\n")

    except Exception as e:
        log_box.insert(tk.END, f"❌ Error: {str(e)}\n")

def open_ui():
    global log_text

    root = tk.Tk()
    root.title("CX IT CSV Generator")
    root.geometry("600x520")

    today = datetime.today()
    this_monday = today - timedelta(days=today.weekday())
    last_monday = this_monday - timedelta(days=7)

    tk.Label(root, text="Start Date").grid(row=0, column=0, padx=10, pady=5, sticky='e')
    start_date = DateEntry(root, width=12, date_pattern='yyyy-mm-dd')
    start_date.set_date(last_monday)
    start_date.grid(row=0, column=1, pady=5, sticky='w')

    tk.Label(root, text="End Date").grid(row=1, column=0, padx=10, pady=5, sticky='e')
    end_date = DateEntry(root, width=12, date_pattern='yyyy-mm-dd')
    end_date.set_date(this_monday)
    end_date.grid(row=1, column=1, pady=5, sticky='w')

    tk.Label(root, text="Rows per CSV").grid(row=2, column=0, padx=10, pady=5, sticky='e')
    rows_per_file = tk.Entry(root)
    rows_per_file.insert(0, "19")
    rows_per_file.grid(row=2, column=1, pady=5, sticky='w')

    tk.Label(root, text="Output Folder").grid(row=3, column=0, padx=10, pady=5, sticky='e')
    output_path = tk.StringVar(value=os.path.join(os.path.expanduser("~"), "Desktop"))
    tk.Entry(root, textvariable=output_path, width=30).grid(row=3, column=1, pady=5, sticky='w')
    tk.Button(root, text="Browse", command=lambda: output_path.set(filedialog.askdirectory())).grid(row=3, column=2, pady=5, padx=5)

    tk.Label(root, text="Filter by Assignee").grid(row=4, column=0, padx=10, pady=5, sticky='e')
    assignee_var = tk.StringVar()
    assignee_dropdown = ttk.Combobox(root, textvariable=assignee_var, state="readonly")
    assignee_dropdown.grid(row=4, column=1, pady=5, sticky='w')

    log_text = scrolledtext.ScrolledText(root, wrap=tk.WORD, width=70, height=12)
    log_text.grid(row=6, column=0, columnspan=3, padx=10, pady=(15, 10))

    df_loaded = load_data()
    unique_assignees = sorted(df_loaded['Assignee1'].dropna().unique().tolist())
    assignee_dropdown['values'] = ["All"] + unique_assignees
    assignee_dropdown.set("All")

    def run_process():
        s_date = start_date.get()
        e_date = end_date.get()
        rows = rows_per_file.get()
        path = output_path.get()
        assignee = assignee_var.get()

        if not path:
            messagebox.showwarning("Missing Folder", "Please choose an output folder.")
            return

        try:
            rows_int = int(rows)
        except ValueError:
            messagebox.showerror("Invalid Input", "Rows per CSV must be an integer.")
            return

        log_text.insert(tk.END, f"🔍 Filtering data from {s_date} to {e_date} for {assignee}...\n")
        log_text.see(tk.END)

        process_and_save(s_date, e_date, rows_int, path, df_loaded, log_text, assignee)

    tk.Button(root, text="Generate CSVs", command=run_process, bg="green", fg="white", width=20).grid(row=5, column=1, pady=15)

    root.mainloop()

open_ui()
