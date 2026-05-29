#!/usr/bin/env bash
set -euo pipefail

ALL_LABELS=(
	"nexmark.bench.datagen=1"
	"arroyo.nexmark.bench=1"
	"flink.nexmark.bench=1"
	"risingwave.nexmark.bench=1"
	"datalayers.nexmark.bench=1"
)

NAME_PREFIXES=(
	"nexmark-datagen-kafka-"
	"arroyo-nexmark-kafka-"
	"arroyo-nexmark-"
	"flink-nexmark-kafka-"
	"risingwave-nexmark-kafka-"
	"risingwave-nexmark-standalone-"
	"datalayers-nexmark-kafka-"
	"datalayers-nexmark-flink-cache-"
)

NETWORK_PREFIXES=(
	"arroyo-nexmark-net-"
	"risingwave-nexmark-net-"
)

echo "=== Cleaning Nexmark bench containers and networks ==="

filter_args=()
for label in "${ALL_LABELS[@]}"; do
	filter_args+=(--filter "label=${label}")
done

mapfile -t container_ids < <(docker ps -a -q "${filter_args[@]}" 2>/dev/null || true)

if [[ ${#container_ids[@]} -eq 0 ]]; then
	echo "No containers found via labels, trying name prefix fallback..."

	container_ids=()
	mapfile -t all_ids < <(docker ps -a -q 2>/dev/null || true)
	for id in "${all_ids[@]}"; do
		name=$(docker inspect --format '{{.Name}}' "$id" 2>/dev/null | sed 's|^/||')
		for prefix in "${NAME_PREFIXES[@]}"; do
			if [[ "$name" == "$prefix"* ]]; then
				container_ids+=("$id")
				break
			fi
		done
	done
fi

if [[ ${#container_ids[@]} -gt 0 ]]; then
	echo "Stopping and removing ${#container_ids[@]} container(s)..."
	docker rm -f "${container_ids[@]}"
else
	echo "No matching containers found."
fi

echo "=== Cleaning networks ==="
mapfile -t all_networks < <(docker network ls --format '{{.Name}}' 2>/dev/null || true)
net_removed=0
for net in "${all_networks[@]}"; do
	for prefix in "${NETWORK_PREFIXES[@]}"; do
		if [[ "$net" == "$prefix"* ]]; then
			echo "Removing network: $net"
			docker network rm "$net" 2>/dev/null || true
			((net_removed++)) || true
			break
		fi
	done
done

if [[ $net_removed -eq 0 ]]; then
	echo "No matching networks found."
fi

echo "=== Done ==="
