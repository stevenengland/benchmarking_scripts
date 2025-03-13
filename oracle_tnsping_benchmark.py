import re
import socket
import time
import statistics
import argparse

packet = (
        b"\x00W\x00\x00\x01\x00\x00\x00\x018\x01,\x00\x00\x08\x00\x7f\xff"
        b"\x7f\x08\x00\x00\x01\x00\x00\x1d\x00:\x00\x00\x00\x00\x00\x00"
        b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x190\x00\x00\x00\x8d"
        b"\x00\x00\x00\x00\x00\x00\x00\x00(CONNECT_DATA=(COMMAND=ping))"
    )

pattern = r"\(DESCRIPTION=\(TMP=\)\(VSNNUM=0\)\(ERR=0\)\(ALIAS=.*?\)\)"

def measure_latency(host, port=80, count=10, timeout=2, wait=0.5, include_conn_setup=False):
    latencies = []
    failed_attempts = 0
    
    print(f"Measuring latency: {host}:{port} ({count} attempts, {timeout}s timeout, {wait}s wait, include_conn_setup {include_conn_setup})...")
    
    for i in range(count):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: 
                s.settimeout(timeout)
                received = "no data"
                
                start_time = time.perf_counter() if include_conn_setup else None
                s.connect((host, port))
                if not include_conn_setup:
                    start_time = time.perf_counter()

                s.send(packet)
                while True:
                    data = s.recv(4096)
                    if not data:
                        break
                    received = data[12:].decode()
                
                end_time = time.perf_counter()
                
                s.close()
                
                latency = (end_time - start_time) * 1000
                latencies.append(latency)

                if not re.match(pattern, received):
                    raise ValueError(f"Wrong TNSPing answer received: {received}")
                
                print(f"  Attempt {i+1}/{count}: {latency:.2f} ms.")

                time.sleep(wait)
            
        except socket.timeout:
            print(f"  Attempt {i+1}/{count}: Timed out after {timeout} seconds")
            failed_attempts += 1
            
        except Exception as e:
            print(f"  Attempt {i+1}/{count}: Error - {e}")
            failed_attempts += 1
            time.sleep(wait)
    
    if latencies:
        stats = {
            "min": min(latencies),
            "max": max(latencies),
            "mean": statistics.mean(latencies),
            "median": statistics.median(latencies),
            "stdev": statistics.stdev(latencies) if len(latencies) > 1 else 0,
            "successful_attempts": len(latencies),
            "failed_attempts": failed_attempts
        }
        return stats
    else:
        return None

def main():
    parser = argparse.ArgumentParser(description='Measure network latency to a host')
    parser.add_argument('host', type=str, help='Target hostname or IP address')
    parser.add_argument('-p', '--port', type=int, default=80, help='Target port number (default: 80)')
    parser.add_argument('-c', '--count', type=int, default=10, help='Number of measurements to take (default: 10)')
    parser.add_argument('-t', '--timeout', type=float, default=2, help='Socket timeout in seconds (default: 2)')
    parser.add_argument('-w', '--wait', type=float, default=0.5, help='Wait time between each attempt (default: 0.5)')
    parser.add_argument('-i', '--include-conn-setup', action="store_true", default=False, help='Include the connection setup before sending the ping into the measurement? (default: False)')
    
    args = parser.parse_args()
    
    results = measure_latency(args.host, args.port, args.count, args.timeout, args.wait, args.include_conn_setup)
    
    if results:
        print("\nResults:")
        print(f"  Minimum latency: {results['min']:.2f} ms")
        print(f"  Maximum latency: {results['max']:.2f} ms")
        print(f"  Mean latency: {results['mean']:.2f} ms")
        print(f"  Median latency: {results['median']:.2f} ms")
        print(f"  Standard deviation: {results['stdev']:.2f} ms")
        print(f"  Success rate: {results['successful_attempts']}/{results['successful_attempts'] + results['failed_attempts']} attempts")
    else:
        print(f"Failed to measure latency to {args.host}:{args.port}")

if __name__ == "__main__":
    main()