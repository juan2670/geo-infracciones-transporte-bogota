# Geo-Infracciones — Transporte Bogotá

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![GeoPandas](https://img.shields.io/badge/GeoPandas-ok-success)
![Shapely](https://img.shields.io/badge/Shapely-ok-informational)
![Folium](https://img.shields.io/badge/Folium-map-green)


Este proyecto automatiza el procesamiento de registros de infracciones viales, vinculando cada evento a su ubicación geográfica precisa, la ruta de bus más cercana y el paradero relevante en sistemas de transporte público.


---

## 👀 ¿Qué hace?

- **Geocodificación** automática de direcciones usando APIs de mapas (Here, Nominatim).
- **Detecta** si la infracción ocurrió **dentro/fuera** del trazado.  
- **Genera mapas** interactivos (HTML) con capas por **tipo de evento**, **ruta** y **fecha**.  
- **Exporta** resultados a **CSV** para análisis posterior.
- **Procesamiento incremental** sólo se procesan y georreferencian las nuevas direcciones agregadas.

---

## 🗺️ Datos

- Puntos de infracción: `data/eventos.csv` *(id, fecha, lat, lon, tipo)*  
- Rutas: `rutas/*.geojson` o `rutas/*.kml`  

> Ajusta las rutas en `config.yml` o variables del script principal.
