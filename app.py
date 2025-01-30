from flask import Flask, render_template, request, send_file, session, url_for
import pandas as pd
import os
import tempfile
import uuid
import logging
from io import BytesIO

app = Flask(__name__)
app.secret_key = os.urandom(24)
app.config['UPLOAD_FOLDER'] = tempfile.mkdtemp()
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        try:
            # Get uploaded files
            reference_file = request.files.get('reference')
            data_files = request.files.getlist('data_files')

            if not reference_file or not data_files:
                return "Please upload a reference file and at least one data file.", 400

            # Read reference file (CSV or Excel)
            if reference_file.filename.endswith('.csv'):
                reference_df = pd.read_csv(reference_file, dtype=str)
            elif reference_file.filename.endswith(('.xls', '.xlsx')):
                reference_df = pd.read_excel(reference_file, dtype=str)
            else:
                return "Reference file must be a CSV or Excel file.", 400

            # Read data files (CSV or Excel)
            data_dfs = []
            for file in data_files:
                if file.filename.endswith('.csv'):
                    data_dfs.append(pd.read_csv(file, dtype=str))
                elif file.filename.endswith(('.xls', '.xlsx')):
                    data_dfs.append(pd.read_excel(file, dtype=str))
                else:
                    return f"Invalid data file format: {file.filename}", 400

            if not data_dfs:
                return "No valid data files uploaded.", 400

            # Combine all data files
            combined_data = pd.concat(data_dfs, ignore_index=True)

            # Session management
            session_id = str(uuid.uuid4())
            session.clear()
            session['session_id'] = session_id

            # Save temp files
            temp_data_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{session_id}_data.csv")
            temp_reference_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{session_id}_reference.csv")

            combined_data.to_csv(temp_data_path, index=False)
            reference_df.to_csv(temp_reference_path, index=False)

            return render_template('select_columns.html',
                                   data_columns=combined_data.columns.tolist(),
                                   reference_columns=reference_df.columns.tolist(),
                                   session_id=session_id)

        except Exception as e:
            logger.error(f"Error processing files: {e}")
            return f"Error: {str(e)}", 500

    return render_template('upload.html')

@app.route('/process', methods=['POST'])
def process():
    try:
        session_id = request.form.get('session_id')
        if not session_id or session_id != session.get('session_id'):
            return "Invalid session", 400

        session['data_column'] = request.form['data_column']
        session['reference_column'] = request.form['reference_column']

        return render_template('download_files.html',
                               missing_url=url_for('download_missing'),
                               existing_url=url_for('download_existing'))
    except Exception as e:
        logger.error(f"Processing error: {e}")
        return f"Error processing request: {str(e)}", 500

@app.route('/download_missing')
def download_missing():
    try:
        session_id = session.get('session_id')
        if not session_id:
            return "Session expired", 400

        temp_data_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{session_id}_data.csv")
        temp_reference_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{session_id}_reference.csv")

        data_df = pd.read_csv(temp_data_path, dtype=str)
        reference_df = pd.read_csv(temp_reference_path, dtype=str)

        data_col = session.get('data_column')
        ref_col = session.get('reference_column')

        data_df[data_col] = data_df[data_col].str.strip().str.upper()
        reference_df[ref_col] = reference_df[ref_col].str.strip().str.upper()

        missing_records = data_df[~data_df[data_col].isin(reference_df[ref_col])]

        output = BytesIO()
        missing_records.to_csv(output, index=False)
        output.seek(0)

        return send_file(output, mimetype='text/csv', as_attachment=True, download_name="missing_records.csv")
    except Exception as e:
        logger.error(f"Download missing error: {e}")
        return "Error generating missing records", 500

@app.route('/download_existing')
def download_existing():
    try:
        session_id = session.get('session_id')
        if not session_id:
            return "Session expired", 400

        temp_data_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{session_id}_data.csv")
        temp_reference_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{session_id}_reference.csv")

        data_df = pd.read_csv(temp_data_path, dtype=str)
        reference_df = pd.read_csv(temp_reference_path, dtype=str)

        data_col = session.get('data_column')
        ref_col = session.get('reference_column')

        data_df[data_col] = data_df[data_col].str.strip().str.upper()
        reference_df[ref_col] = reference_df[ref_col].str.strip().str.upper()

        existing_records = data_df[data_df[data_col].isin(reference_df[ref_col])]

        output = BytesIO()
        existing_records.to_csv(output, index=False)
        output.seek(0)

        return send_file(output, mimetype='text/csv', as_attachment=True, download_name="existing_records.csv")
    except Exception as e:
        logger.error(f"Download existing error: {e}")
        return "Error generating existing records", 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))