import os
import tempfile
import json
import re
import pandas as pd
import tabula
import pdfplumber
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import asyncio

app = FastAPI(title="FPK Converter API - Streaming")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def ambil_metadata_pdf(pdf_path: str):
    nama_file, tingkat = "Hasil_Konversi_FPK", "UNKNOWN"
    try:
        with pdfplumber.open(pdf_path) as pdf:
            text = pdf.pages[0].extract_text() or ""
            bulan_pola = (r"(JANUARI|FEBRUARI|MARET|APRIL|MEI|JUNI|JULI|"
                          r"AGUSTUS|SEPTEMBER|OKTOBER|NOVEMBER|DESEMBER)")
            m_b = re.search(f"{bulan_pola}\\s+(\\d{{4}})", text, re.IGNORECASE)
            m_t = re.search(r"Tingkat\s+Pelayanan\s*:\s*(RITL|RJTL|RITP|RJTP)", text, re.IGNORECASE)
            if m_b:
                bulan = m_b.group(1).upper()
                tahun = m_b.group(2)
                tingkat = m_t.group(1).upper() if m_t else "FPK"
                nama_file = f"FPK_{tingkat}_{bulan}_{tahun}"
            elif m_t:
                tingkat = m_t.group(1).upper()
                nama_file = f"FPK_{tingkat}"
    except:
        pass
    return nama_file, tingkat

async def generate_stream(file: UploadFile):
    """Generator untuk streaming per baris."""
    tmp_path = None
    try:
        content = await file.read()
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(content)
            tmp_path = tmp.name

        # Kirim metadata dulu
        nama, tingkat = ambil_metadata_pdf(tmp_path)
        yield json.dumps({"type": "metadata", "tingkat": tingkat, "filename": nama}) + "\n"

        # Proses tabel per halaman, streaming per baris
        df_list = tabula.read_pdf(tmp_path, pages='all', multiple_tables=True,
                                  lattice=True, pandas_options={'header': None})
        total_data = 0
        total_nominal = 0
        for df in df_list:
            if df.shape[1] < 6:
                continue
            df_data = df.iloc[:, :6].copy()
            df_data = df_data[pd.to_numeric(df_data.iloc[:, 0], errors='coerce').notna()]
            if df_data.empty:
                continue
            df_data.columns = ['No. Urut', 'No.SEP', 'Tgl. Verifikasi', 'Biaya Riil RS', 'Diajukan', 'Disetujui']
            df_data['No.SEP'] = (df_data['No.SEP'].astype(str)
                                 .str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.strip())
            df_data['Disetujui'] = (pd.to_numeric(
                df_data['Disetujui'].astype(str).str.replace(r'[^0-9]', '', regex=True),
                errors='coerce').fillna(0).astype(int))
            # Streaming per baris
            for _, row in df_data.iterrows():
                yield json.dumps({
                    "type": "data",
                    "No.SEP": row['No.SEP'],
                    "Disetujui": int(row['Disetujui'])
                }) + "\n"
                total_data += 1
                total_nominal += int(row['Disetujui'])
                await asyncio.sleep(0.001)  # kecil agar tidak overload

        # Kirim status selesai
        yield json.dumps({
            "type": "done",
            "total": total_nominal,
            "jumlah": total_data
        }) + "\n"

    except Exception as e:
        yield json.dumps({"type": "error", "message": str(e)}) + "\n"
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)

@app.post("/api/proses-stream")
async def proses_stream(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="File harus PDF")
    return StreamingResponse(
        generate_stream(file),
        media_type="application/x-ndjson"
    )

@app.get("/api/health")
async def health():
    return {"status": "ok"}
