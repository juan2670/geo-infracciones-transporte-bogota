Este proyecto automatiza el procesamiento de registros de infracciones viales, vinculando cada evento a su ubicación geográfica precisa, la ruta de bus más cercana y el paradero relevante en sistemas de transporte público.

**Características principales:**
- Geocodificación automática de direcciones usando APIs de mapas (Here, Nominatim).
- Asociación inteligente de cada infracción con la ruta y el paradero más cercanos (análisis espacial con árbol de decisión).
- Exportación consolidada en un solo archivo Excel, compatible con análisis posteriores en Power BI o GIS.
- Procesamiento incremental: sólo se procesan y georreferencian las nuevas direcciones agregadas.
- Visualización opcional de rutas, puntos y paraderos para auditoría y control de calidad.

