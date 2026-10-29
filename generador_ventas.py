"""
generador_ventas.py

Genera un archivo CSV con datos de ventas ficticias para practicar
procesamiento de datos con Python, Hadoop y Apache Spark.

Uso:
    python generador_ventas.py
    python generador_ventas.py --registros 1000 --salida ventas.csv
"""

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path


PRODUCTOS = [
    ("P001", "Nevera", "Electrodomesticos", 1200000, 2200000),
    ("P002", "Lavadora", "Electrodomesticos", 900000, 1800000),
    ("P003", "Televisor", "Tecnologia", 700000, 2500000),
    ("P004", "Sofa", "Hogar", 500000, 1600000),
    ("P005", "Comedor", "Hogar", 400000, 1300000),
    ("P006", "Portatil", "Tecnologia", 1500000, 3500000),
    ("P007", "Licuadora", "Electrodomesticos", 80000, 300000),
]

CIUDADES = ["Bogota", "Medellin", "Cali", "Villavicencio", "Bucaramanga"]


def generar_ventas(cantidad, archivo_salida):
    """Crea registros aleatorios y los guarda en un archivo CSV."""
    fecha_inicial = date(2026, 1, 1)
    fecha_final = date(2026, 10, 29)
    dias = (fecha_final - fecha_inicial).days

    columnas = [
        "id_venta",
        "fecha",
        "id_cliente",
        "ciudad",
        "id_producto",
        "producto",
        "categoria",
        "cantidad",
        "precio_unitario",
        "total",
    ]

    ruta = Path(archivo_salida)
    ruta.parent.mkdir(parents=True, exist_ok=True)

    with ruta.open("w", newline="", encoding="utf-8") as archivo:
        escritor = csv.DictWriter(archivo, fieldnames=columnas)
        escritor.writeheader()

        for numero in range(1, cantidad + 1):
            codigo, producto, categoria, minimo, maximo = random.choice(PRODUCTOS)
            unidades = random.randint(1, 5)
            precio = random.randrange(minimo, maximo + 1, 1000)
            fecha = fecha_inicial + timedelta(days=random.randint(0, dias))

            escritor.writerow({
                "id_venta": numero,
                "fecha": fecha.isoformat(),
                "id_cliente": f"C{random.randint(1, 200):04d}",
                "ciudad": random.choice(CIUDADES),
                "id_producto": codigo,
                "producto": producto,
                "categoria": categoria,
                "cantidad": unidades,
                "precio_unitario": precio,
                "total": unidades * precio,
            })

    print(f"Se generaron {cantidad} ventas en: {ruta.resolve()}")


def main():
    parser = argparse.ArgumentParser(description="Generador de datos de ventas")
    parser.add_argument("--registros", type=int, default=500,
                        help="Cantidad de ventas que se van a generar")
    parser.add_argument("--salida", default="ventas.csv",
                        help="Nombre o ruta del archivo CSV de salida")
    args = parser.parse_args()

    if args.registros < 1:
        parser.error("--registros debe ser mayor que cero")

    generar_ventas(args.registros, args.salida)


if __name__ == "__main__":
    main()
