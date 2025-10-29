# Geo-Infracciones — Transporte Bogotá

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![GeoPandas](https://img.shields.io/badge/GeoPandas-ok-success)
![Shapely](https://img.shields.io/badge/Shapely-ok-informational)
![Folium](https://img.shields.io/badge/Folium-map-green)


Proyecto para **analizar y visualizar geográficamente** infracciones del sistema de transporte en Bogotá usando **GeoPandas**, **Shapely**, **Folium** y **SQLite**.

---

## 👀 ¿Qué hace?

- **Asocia** cada evento (lat, lon) a la **ruta** o **tramo** más cercano.  
- **Detecta** si la infracción ocurrió **dentro/fuera** del trazado.  
- **Genera mapas** interactivos (HTML) con capas por **tipo de evento**, **ruta** y **fecha**.  
- **Exporta** resultados a **CSV/GeoJSON** para análisis posterior.

---

## 🗺️ Datos

- Puntos de infracción: `data/eventos.csv` *(id, fecha, lat, lon, tipo)*  
- Rutas: `rutas/*.geojson` o `rutas/*.kml`  
- (Opcional) BD: `data/infractions.db`

> Ajusta las rutas en `config.yml` o variables del script principal.

---

## ⚙️ Requisitos

```bash
python -m pip install --upgrade pip
python -m pip install geopandas shapely folium pandas matplotlib
