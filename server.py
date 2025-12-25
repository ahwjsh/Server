import zmq
import os
import psycopg2
import json
import re
from datetime import datetime

conn = psycopg2.connect(dbname="db_android_app", host="localhost", user="postgres", password="12345", port="5432")
cursor = conn.cursor()
context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:2222")
print("Ожидание данных")

def parse_data(data_string):
    try:
        # Ищем данные Location
        lat = lon = alt = 0.0
        loc_time = None
        
        # Парсим Location
        if "Location:" in data_string:
            loc_part = data_string.split("Location:")[1].split(";")[0].strip()
            parts = [p.strip() for p in loc_part.split(",")]
            if len(parts) >= 4:
                lat = float(parts[0])
                lon = float(parts[1])
                alt = float(parts[2])
                loc_time = parts[3]
        
        # Ищем данные CellInfoLte
        mcc = mnc = pci = rsrp = rsrq = rssi = 0
        
        if "CellInfoLte:" in data_string:
            cell_part = data_string.split("CellInfoLte:")[1].strip()
            cells = [c.strip() for c in cell_part.split(",")]
            if len(cells) >= 6:
                mcc = int(cells[0])
                mnc = int(cells[1])
                pci = int(cells[2])
                rsrp = int(cells[3])
                rsrq = int(cells[4])
                rssi = int(cells[5])
        
        return {
            'latitude': lat,
            'longitude': lon,
            'altitude': alt,
            'location_timestamp': loc_time,
            'mcc': mcc,
            'mnc': mnc,
            'pci': pci,
            'rsrp': rsrp,
            'rsrq': rsrq,
            'rssi': rssi
        }
    except Exception as e:
        print(f"Ошибка парсинга: {e}")
        return None
    
try:
    while True:
        # Получаем данные от Android
        message = socket.recv_string()
        print(f"\nПолучены данные: {message}")
        
        # Получаем текущее время
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Парсим данные
        parsed = parse_data(message)
        
        if parsed:
            # Сохраняем в базу данных
            cursor.execute('''
            INSERT INTO datta 
            (received_at, latitude, longitude, altitude, location_timestamp, 
             mcc, mnc, pci, rsrp, rsrq, rssi)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                current_time,
                parsed['latitude'],
                parsed['longitude'],
                parsed['altitude'],
                parsed['location_timestamp'],
                parsed['mcc'],
                parsed['mnc'],
                parsed['pci'],
                parsed['rsrp'],
                parsed['rsrq'],
                parsed['rssi']
            ))
            conn.commit()
            
            # Получаем ID последней записи
            cursor.execute("SELECT lastval()")
            record_id = cursor.fetchone()[0]
            
            print(f" Данные сохранены в таблицу 'datta'. ID записи: {record_id}")
            
            # Отправляем ответ Android
            socket.send_string(f"Данные сохранены ID: {record_id}")
            
            # Сохраняем также в JSON файл 
            with open("android_data.json", "a", encoding="utf-8") as f:
                json.dump({
                    "id": record_id,
                    "received_at": current_time,
                    "data": message
                }, f, ensure_ascii=False)
                f.write("\n")
            
        else:
            print(" Ошибка парсинга данных")
            socket.send_string("Ошибка: неверный формат данных")
            
except KeyboardInterrupt:
    print("\n\nСервер остановлен")
    
finally:
    # Закрываем соединения
    cursor.close()

conn.close()
socket.close()
context.term()
