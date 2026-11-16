#!/bin/sh
set -u
url=https://download.geofabrik.de/europe/france/alsace.poly
echo 'Control HTTPS request:'
curl -4 -sS -o /dev/null -w 'IPv4 control HTTP %{http_code} remote %{remote_ip}\n' --connect-timeout 5 --max-time 12 https://www.openstreetmap.org/ || true
echo 'Geofabrik DNS:'
getent ahostsv4 download.geofabrik.de || true
echo 'Geofabrik by address family:'
curl -4 -sS -o /dev/null -w 'IPv4 HTTP %{http_code} remote %{remote_ip}\n' --connect-timeout 5 --max-time 12 "$url" || true
curl -6 -sS -o /dev/null -w 'IPv6 HTTP %{http_code} remote %{remote_ip}\n' --connect-timeout 5 --max-time 12 "$url" || true
echo 'Geofabrik each IPv4 address:'
for ip in $(getent ahostsv4 download.geofabrik.de | awk '{print $1}' | sort -u); do
  echo "Testing $ip"
  curl -4 -sS -o /dev/null -w 'HTTP %{http_code} remote %{remote_ip}\n' --noproxy '*' --resolve "download.geofabrik.de:443:$ip" --connect-timeout 5 --max-time 12 "$url" || true
done
