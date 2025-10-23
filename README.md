
# ⚡ Reprogramaciones COES

Aplicación **Streamlit** para visualizar y analizar reprogramaciones (PDO/RDO) del COES: **Demanda**, **Hidro/Eólico/Solar**, **CMG por barra**, **Motivos/Costos**, **Índices (Alfa/Beta/Gamma)** y **exporte a PDF**.

---

## ✨ Características
- Lectura de insumos **PDO/RDO** por fecha.
- Gráficos con eje X estandarizado (48 medias horas, rotación 90°, margen lateral) y “aire” en Y.
- Pestañas:
  - **Demanda** + **Error relativo (%)**.
  - **Hidro / Eólico / Solar** + errores relativos (%).
  - **CMG por barra**.
  - **Motivos** (RDO A–F) y **Costo total**.
  - **Índices** (Alfa, Beta, Gamma).
- **Reporte PDF** (ejemplo incluido: `Reporte.pdf`).
- Botón **Generar** y **Descargar PDF** desde la interfaz.

---

## 🧱 Estructura
```

Reprogramaciones_USGE/
├─ app.py
├─ Reporte.pdf           
├─ requirements.txt
└─ README.md

```

---

## 📦 Requisitos
Crea un `requirements.txt` con (ajusta si usas otros paquetes):
```

streamlit>=1.39
pandas>=2.2
numpy>=1.26
matplotlib>=3.8
openpyxl>=3.1
requests>=2.31

````

---

## 🚀 Ejecución local
```bash
# 1) (Opcional) entorno virtual
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/Mac
source .venv/bin/activate

# 2) Instalar dependencias
pip install -r requirements.txt

# 3) Ejecutar la app
streamlit run app.py
````

> Consejo: autoreload al guardar
> `streamlit run app.py --server.runOnSave true`

---

## 🧩 Uso

1. En la **sidebar**, define:

   * **Fecha del reporte**
   * **Inicio del rango** (si aplica)
   * **Carpeta de trabajo** (por defecto: `Desktop/Descargas_T` o una ruta como `C:\Users\TUUSER\Desktop\Descargas_T`)
2. Pulsa **Generar**.
3. Navega por las secciones (**Demanda**, **Hidro/Eólico/Solar**, **CMG**, **Motivos/Costos**, **Índices**).
4. Descarga el **PDF** con el botón **“Descargar PDF”** (si el archivo fue generado).

---

## ⚙️ Detalles técnicos útiles

* **Eje X (horas)**: 48 puntos (00:30…23:59), etiquetas a **90°** y márgenes laterales.
* **Alineado de series**: uso de índices numéricos (0–47) para mantener el mismo formato en todos los gráficos.
* **Solar**: se ocultan ceros fuera de ventanas horarias usando la regla
  `if v == 0 and not (0 <= i <= 11 or 36 <= i <= 47): y_vals.append(None)`.
* **Errores relativos**: comparación secuencial (PDO → RDO A → RDO B → …) con `% abs`.
* **PDF**: se construye agrupando figuras exportadas durante la ejecución.

---

## 🖥️ Despliegue (Streamlit Community Cloud)

1. Sube el repo a **GitHub** (ruta del archivo principal: `app.py`).
2. Ve a **share.streamlit.io** → **New app** → selecciona repo/branch → `app.py`.
3. Si actualizas código: **commit & push** → la app se reconstruye. (Menú ⋮ → *Rerun* / *Clear cache* si no ves cambios).
4. Variables sensibles: **Settings → Secrets**.

---

## 👤 Autora

**Joanna Alexandra Carrión Pérez**  
🎓 Bachiller en Ingeniería Electrónica  
🚀 Apasionada por la ciencia de datos y sistemas inteligentes  
🔗 ![LinkedIn](https://img.shields.io/badge/LinkedIn-Joanna%20Carrión%20Pérez-blue?style=flat&logo=linkedin) [LinkedIn](https://www.linkedin.com/in/joanna-carrion-perez/)

--- 

## 📬 Contacto
📧 **joannacarrion14@gmail.com** 

--- 

## 💡 Contribuciones 
¡Contribuciones son bienvenidas! Si tienes ideas o mejoras, haz un fork del repo y envía un **pull request**. 🚀
