import base64
import socket
import time

HOST = '0.0.0.0'
PORT = 9000
WIDTH = 320
HEIGHT = 180
FPS = 12


def build_frame(frame_id: int) -> bytes:
    pixels = bytearray(WIDTH * HEIGHT * 3)
    for y in range(HEIGHT):
        for x in range(WIDTH):
            i = (y * WIDTH + x) * 3
            pixels[i] = (x + frame_id * 3) % 256
            pixels[i + 1] = (y * 2 + frame_id * 5) % 256
            pixels[i + 2] = ((x + y) + frame_id * 7) % 256

    payload = base64.b64encode(pixels).decode('ascii')
    timestamp = int(time.time() * 1000)
    line = f"{frame_id},{timestamp},{WIDTH},{HEIGHT},{payload}\n"
    return line.encode('utf-8')


def main() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST, PORT))
        server.listen(1)
        print(f"Sample TCP source listening on {HOST}:{PORT}")

        while True:
            conn, addr = server.accept()
            print(f"Client connected: {addr}")
            with conn:
                frame_id = 0
                try:
                    while True:
                        conn.sendall(build_frame(frame_id))
                        frame_id += 1
                        time.sleep(1 / FPS)
                except (BrokenPipeError, ConnectionResetError):
                    print('Client disconnected')


if __name__ == '__main__':
    main()
