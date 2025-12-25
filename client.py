import socket
    
# Создаем сокет
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
# Подключаемся к серверу
client_socket.connect(('127.0.0.1', 8080))
    
# Отправляем данные серверу
client_socket.sendall(b'Hello, server!')
    
# Закрываем соединение
client_socket.close()