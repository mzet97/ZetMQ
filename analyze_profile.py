import gzip
import json
import os
from collections import defaultdict

profile_path = 'profile.json.gz' if os.path.exists('profile.json.gz') else 'profile.json'

if profile_path.endswith('.gz'):
    f = gzip.open(profile_path, 'rt')
else:
    f = open(profile_path, 'r')

with f:
    data = json.load(f)

print('Threads:', len(data.get('threads', [])))

func_self = defaultdict(float)
func_total = defaultdict(float)

for thread in data.get('threads', []):
    samples = thread.get('samples', {})
    length = samples.get('length', 0)
    stacks = samples.get('stack', [])
    weights = samples.get('weight', [])

    if length == 0:
        continue

    string_array = thread.get('stringArray', [])

    func_table = thread.get('funcTable', {})
    func_name_col = func_table.get('name', [])
    func_name_map = {}
    for idx in range(func_table.get('length', 0)):
        name_idx = func_name_col[idx] if idx < len(func_name_col) else None
        if name_idx is not None and 0 <= name_idx < len(string_array):
            func_name_map[idx] = string_array[name_idx]
        else:
            func_name_map[idx] = f'<func {idx}>'

    frame_table = thread.get('frameTable', {})
    frame_func_col = frame_table.get('func', [])
    frame_name_map = {}
    for idx in range(frame_table.get('length', 0)):
        func_idx = frame_func_col[idx] if idx < len(frame_func_col) else None
        frame_name_map[idx] = func_name_map.get(func_idx, f'<frame {idx}>')

    stack_table = thread.get('stackTable', {})
    stack_frame_col = stack_table.get('frame', [])
    stack_prefix_col = stack_table.get('prefix', [])

    for i in range(length):
        stack_idx = stacks[i]
        weight = weights[i] if i < len(weights) else 1

        visited = set()
        current = stack_idx
        stack_frame_indices = []
        while current is not None and current not in visited:
            visited.add(current)
            frame_idx = stack_frame_col[current] if current < len(stack_frame_col) else None
            stack_frame_indices.append(frame_idx)
            current = stack_prefix_col[current] if current < len(stack_prefix_col) else None

        if stack_frame_indices:
            top_frame = stack_frame_indices[0]
            top_name = frame_name_map.get(top_frame, '<unknown>')
            func_self[top_name] += weight

        for frame_idx in stack_frame_indices:
            name = frame_name_map.get(frame_idx, '<unknown>')
            func_total[name] += weight

print('\nTop 20 by self time:')
for name, weight in sorted(func_self.items(), key=lambda x: -x[1])[:20]:
    print(f'  {weight:>12.1f}  {name}')

print('\nTop 20 by total time:')
for name, weight in sorted(func_total.items(), key=lambda x: -x[1])[:20]:
    print(f'  {weight:>12.1f}  {name}')
