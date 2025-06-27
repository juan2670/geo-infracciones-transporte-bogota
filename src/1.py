import pandas as pd
import requests
import time
import re
import geopandas as gpd
from shapely.geometry import Point
import os
import matplotlib.cm as cm
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt

# -------------------- CONFIGURACIÓN --------------------
ARCHIVO_INFRACCIONES = "Infracciones.csv"  # Archivo de entradas con direcciones
ARCHIVO_SALIDA = "Infracciones_geocodificadas.xlsx"  # Salida final consolidada
BASE_RUTAS = r"C:\Users\user\OneDrive\Escritorio\Medidas\Rutas"  # Carpeta principal con subcarpetas de rutas
API_KEY = "-QtCxil3_pcLP5h2UrySWhX_qPGzhhtSeTtJpNcv7nc"  # API Key para Here Maps
BATCH_SIZE = 10  # Número de direcciones nuevas a procesar en cada ejecución

# -------------------------------------------------------
# 1. LEER ARCHIVO DE INFRACCIONES Y DETECTAR COLUMNA DE DIRECCIÓN
# -------------------------------------------------------
df = pd.read_csv(ARCHIVO_INFRACCIONES)
col_dir = None
for col in df.columns:
    if "direc" in col.lower():
        col_dir = col
        break
if not col_dir:
    raise ValueError("No se encontró ninguna columna de dirección.")

# -------------------------------------------------------
# 2. LEER O CREAR ARCHIVO DE SALIDA FINAL
# -------------------------------------------------------
if os.path.exists(ARCHIVO_SALIDA):
    df_final = pd.read_excel(ARCHIVO_SALIDA)
    direcciones_existentes = set(df_final[col_dir].astype(str).str.strip())
else:
    df_final = pd.DataFrame()
    direcciones_existentes = set()

# -------------------------------------------------------
# 3. FILTRAR SOLO LAS DIRECCIONES NUEVAS
# -------------------------------------------------------
filas_nuevas = df[~df[col_dir].astype(str).str.strip().isin(direcciones_existentes)].copy()
if len(filas_nuevas) > BATCH_SIZE:
    filas_nuevas = filas_nuevas.head(BATCH_SIZE)

# -------------------------------------------------------
# 4. FUNCIÓN DE LIMPIEZA DE DIRECCIONES
# -------------------------------------------------------
def limpiar_direccion(direccion):
    if pd.isna(direccion) or not isinstance(direccion, str):
        return ""
    direccion = direccion.strip()
    direccion = direccion.replace("Av.", "Avenida").replace("Cra.", "Carrera").replace("Cl.", "Calle")
    direccion = direccion.replace("Kr", "Carrera").replace("No.", "#").replace("N°", "#").replace("&", "#")
    direccion = direccion + ", Bogotá, Colombia"
    direccion = re.sub(r"\b(despues|después|cerca|antes|vía|via|camino|hacia|pasando)\b.*", "", direccion, flags=re.IGNORECASE)
    return direccion

# -------------------------------------------------------
# 5. GEOCODIFICACIÓN DE NUEVAS DIRECCIONES (Here Maps)
# -------------------------------------------------------
for i, row in filas_nuevas.iterrows():
    direccion_limpia = limpiar_direccion(row[col_dir])
    if not direccion_limpia:
        continue
    params = {"q": direccion_limpia, "apiKey": API_KEY}
    try:
        response = requests.get("https://geocode.search.hereapi.com/v1/geocode", params=params)
        data = response.json()
        if data.get("items"):
            filas_nuevas.at[i, "latitud"] = data["items"][0]["position"]["lat"]
            filas_nuevas.at[i, "longitud"] = data["items"][0]["position"]["lng"]
            print(f"✅ {direccion_limpia} → lat: {filas_nuevas.at[i, 'latitud']}, lon: {filas_nuevas.at[i, 'longitud']}")
        else:
            print(f"❌ No se encontró: {direccion_limpia}")
            filas_nuevas.at[i, "latitud"] = None
            filas_nuevas.at[i, "longitud"] = None
    except Exception as e:
        print(f"⚠️ Error con {direccion_limpia}: {e}")
        filas_nuevas.at[i, "latitud"] = None
        filas_nuevas.at[i, "longitud"] = None
    time.sleep(1)  # Evita sobrecargar la API

filas_nuevas = filas_nuevas.dropna(subset=["latitud", "longitud"])

# -------------------------------------------------------
# 6. CARGAR RUTAS Y PARADEROS (geojson/json)
# -------------------------------------------------------
gdf_rutas, gdf_paraderos = [], []
for subcarpeta in os.listdir(BASE_RUTAS):
    subruta = os.path.join(BASE_RUTAS, subcarpeta)
    if os.path.isdir(subruta):
        for archivo in os.listdir(subruta):
            if archivo.endswith((".geojson", ".json")):
                ruta_path = os.path.join(subruta, archivo)
                try:
                    gdf = gpd.read_file(ruta_path).to_crs(epsg=4326)
                    # Detecta si es línea (ruta) o punto (paradero)
                    if gdf.geometry.iloc[0].geom_type in ["LineString", "MultiLineString"]:
                        # Busca nombre de vía amigable
                        nombre_via = None
                        for c in gdf.columns:
                            if "via" in c.lower() or "name" in c.lower() or "descripcion" in c.lower():
                                nombre_via = str(gdf[c].iloc[0])
                                break
                        if not nombre_via:
                            nombre_via = os.path.splitext(archivo)[0]
                        gdf["nombre_ruta"] = subcarpeta
                        gdf["via_ruta"] = nombre_via
                        gdf_rutas.append(gdf)
                    elif gdf.geometry.iloc[0].geom_type == "Point":
                        gdf["nombre_ruta"] = subcarpeta
                        gdf["via_ruta"] = os.path.splitext(archivo)[0]
                        gdf_paraderos.append(gdf)
                except Exception as e:
                    print(f"⚠️ Error cargando {archivo}: {e}")

rutas_gdf = pd.concat(gdf_rutas, ignore_index=True)
paraderos_gdf = pd.concat(gdf_paraderos, ignore_index=True)

# -------------------------------------------------------
# 7. ASOCIACIÓN DE CADA PUNTO A RUTA Y PARADERO MÁS CERCANO
# -------------------------------------------------------
geometry = [Point(xy) for xy in zip(filas_nuevas["longitud"], filas_nuevas["latitud"])]
coords_gdf = gpd.GeoDataFrame(filas_nuevas, geometry=geometry, crs="EPSG:4326")

coords_gdf["Ruta más cercana"] = None
coords_gdf["Via de la ruta"] = None
coords_gdf["Paradero más cercano"] = None
coords_gdf["lat_paradero"] = None
coords_gdf["lon_paradero"] = None
coords_gdf["Distancia (m)"] = None

for idx, punto in coords_gdf.iterrows():
    punto_geom = punto.geometry
    # Buscar la ruta más cercana de todas (Árbol de decisión simple)
    min_dist_ruta = float("inf")
    mejor_ruta, mejor_via, mejor_linea_id = None, None, None
    for _, ruta in rutas_gdf.iterrows():
        d = punto_geom.distance(ruta.geometry)
        if d < min_dist_ruta:
            min_dist_ruta = d
            mejor_ruta = ruta["nombre_ruta"]
            mejor_via = ruta["via_ruta"]
            mejor_linea_id = ruta.name
    # Buscar el paradero más cercano solo de esa ruta
    paraderos_subruta = paraderos_gdf[paraderos_gdf["nombre_ruta"] == mejor_ruta]
    min_dist_paradero = float("inf")
    mejor_paradero, mejor_geom_paradero = None, None
    for _, paradero in paraderos_subruta.iterrows():
        d = punto_geom.distance(paradero.geometry)
        if d < min_dist_paradero:
            min_dist_paradero = d
            # Buscar nombre amigable del paradero
            posible_nombre = None
            for c in paradero.index:
                if "paradero" in c.lower() or "name" in c.lower() or "descripcion" in c.lower():
                    posible_nombre = str(paradero[c])
                    break
            if not posible_nombre:
                posible_nombre = "Sin nombre"
            mejor_paradero = posible_nombre
            mejor_geom_paradero = paradero.geometry
    coords_gdf.at[idx, "Ruta más cercana"] = mejor_ruta
    coords_gdf.at[idx, "Via de la ruta"] = mejor_via
    coords_gdf.at[idx, "Paradero más cercano"] = mejor_paradero
    if mejor_geom_paradero:
        coords_gdf.at[idx, "lat_paradero"] = mejor_geom_paradero.y
        coords_gdf.at[idx, "lon_paradero"] = mejor_geom_paradero.x
        coords_gdf.at[idx, "Distancia (m)"] = round(punto_geom.distance(mejor_geom_paradero) * 111000, 2)

# -------------------------------------------------------
# 8. ACTUALIZAR Y GUARDAR EL ARCHIVO FINAL ÚNICO (EVITA DUPLICADOS)
# -------------------------------------------------------
if not df_final.empty:
    df_final = pd.concat([df_final, coords_gdf], ignore_index=True)
else:
    df_final = coords_gdf

columnas_salida = [col_dir, "latitud", "longitud",
                   "Ruta más cercana", "Via de la ruta",
                   "Paradero más cercano", "lat_paradero", "lon_paradero", "Distancia (m)"]
df_final = df_final.drop_duplicates(subset=[col_dir, "latitud", "longitud"])
df_final.to_excel(ARCHIVO_SALIDA, index=False, columns=columnas_salida)
print(f"✅ Archivo actualizado: {ARCHIVO_SALIDA}")

# -------------------------------------------------------
# 9. VISUALIZACIÓN DE RUTAS, PARADEROS E INFRACCIONES
# -------------------------------------------------------
fig, ax = plt.subplots(figsize=(12, 12))
nombres_ruta = rutas_gdf["nombre_ruta"].unique()
colormap = cm.get_cmap("tab20", len(nombres_ruta))
colores_rutas = {nombre: colormap(i) for i, nombre in enumerate(nombres_ruta)}
for nombre, color in colores_rutas.items():
    subconjunto = rutas_gdf[rutas_gdf["nombre_ruta"] == nombre]
    subconjunto.plot(ax=ax, linewidth=2, color=color, label=nombre, alpha=0.7)
paraderos_gdf.plot(
    ax=ax, marker="o", color="black", markersize=35, alpha=0.7, label="Paraderos"
)
colormap_puntos = cm.get_cmap('hsv', len(coords_gdf))
for idx, row in coords_gdf.iterrows():
    punto = row.geometry
    color = colormap_puntos(idx)
    ax.scatter(punto.x, punto.y, color=color, s=60, zorder=3)
    ax.annotate(
        text=f"{row[col_dir]}\n{row['Via de la ruta']}\n{row['Paradero más cercano']}",
        xy=(punto.x, punto.y),
        xytext=(0, 8),
        textcoords="offset points",
        ha='center',
        fontsize=8,
        bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="black", lw=0.5, alpha=0.9),
        zorder=4
    )
    if pd.notnull(row["lat_paradero"]) and pd.notnull(row["lon_paradero"]):
        ax.plot(
            [punto.x, row["lon_paradero"]],
            [punto.y, row["lat_paradero"]],
            color="red", linestyle="--", linewidth=1.5, alpha=0.6, zorder=2
        )
leyenda_rutas = [mpatches.Patch(color=color, label=nombre) for nombre, color in colores_rutas.items()]
ax.legend(handles=leyenda_rutas + [mpatches.Patch(color="black", label="Paraderos")], loc="upper left", title="Rutas y Paraderos")
ax.set_title("Infracciones vs. Rutas y Paraderos", fontsize=14)
ax.set_xlabel("Longitud")
ax.set_ylabel("Latitud")
ax.grid(True)
ax.axis("equal")
plt.tight_layout()
plt.show()
