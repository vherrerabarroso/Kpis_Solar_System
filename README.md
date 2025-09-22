# Kpis Solar System

Este desarrollo calcula los KPIs principales de un parque solar:

- **Disponibilidad (Av)**
- **Performance Ratio (PR)**

## Fuente de datos
- Los datos provienen de la base de datos de **Industrial Insights**.
- Se obtienen desde la API local:  
  `http://127.0.0.1:8000/solar-system/av-pr`

## Ejecución
- El proceso se programa para correr **cada hora a hh:14:59** (zona horaria: America/Bogota).

## Salida

Cada ejecución genera un JSON con marca de tiempo (`ts`) y los valores calculados en `inc_data`.

**Disponibilidad (Av):**
```json
{
  "ts": "2025-09-22T13:50:44.406000-05:00",
  "inc_data": {
    "AinvPM01": 100.0,
    "AinvPM02": 100.0,
    "AinvPM03": 79.78,
    "AinvPM05": 35.07,
    "AinvPM06": 8.79,
    "av": 76.83
  }
}
```
En este caso:  
- `av` representa la **disponibilidad global del parque solar**.  
- `AinvPMxx` muestra la **disponibilidad individual de cada inversor** dentro de la ventana analizada.  

**Performance Ratio (PR):**
```json
{
  "ts": "2025-09-22T13:50:44.406000-05:00",
  "inc_data": {
    "PrPM01": 68.39,
    "PrPM02": 95.29,
    "PrPM03": 77.46,
    "PrPM05": 90.11,
    "PrPM06": 8.71,
    "pr": 70.94
  }
}
```
En este caso:  
- `pr` representa el **performance ratio global del parque solar**.  
- `PrPMxx` muestra el **performance ratio calculado para cada inversor** en la ventana analizada.  

## Objetivo
La información calculada se envía a **Industrial Insight** para su integración.
