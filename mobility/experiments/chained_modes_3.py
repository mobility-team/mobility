import itertools
import random
import pandas as pd

locations = ["home_start", "school", "work", "shopping", "work", "other", "leisure", "act1", "act2", "leisure", "other", "act3", "other", "home_end"]
# trips = ["home_start", "school", "work", "home_end"]

# trips = [
#     "home_start",  # start
#     "a",
#     "b",
#     "c",
#     "b",   # revisiting b (cycle: b -> c -> b)
#     "d",
#     "a",   # revisiting a (cycle: a -> b -> c -> b -> d -> a)
#     "home_end"
# ]

# locations = [
#     "home_start", "a",
#     "b", "c", "d", "b",
#     "e", "f", "act", "f", "c",
#     "g", "home_end"
# ]

locations = [
    "home_start", "a", "b", "a", "b", "a", "home_end"
]

# locations = ["home", "a", "b", "a", "home"]

last_seen = {}
subtours = []
for end_idx, place in enumerate(locations):
    if place in last_seen:
        start_idx = last_seen[place]
        subtours.append(list(range(start_idx, end_idx+1)))
    last_seen[place] = end_idx


possible_modes = []

def is_mode_available(locations, current_modes, subtour, subtour_mode):

    new_modes = current_modes.copy()
    
    for j in subtour[:-1]:
        new_modes[j] = subtour_mode
        
    car_location = locations[0]
    
    for i in range(len(new_modes)):
        
        if new_modes[i] == "car":
            
            print(f"need to move car from {locations[i]} to {locations[i+1]}")
            
            if car_location != locations[i]:
                return False
            
            car_location = locations[i+1]
            
            
    return True
            
current_modes = ["car"]*(len(locations)-1)
subtour = [1, 2, 3]
subtour_mode = "walk"

is_mode_available(locations, current_modes, subtour, subtour_mode)

current_modes = ['car', 'walk', 'walk', 'car', 'car', 'car']
subtour = [3, 4, 5]
subtour_mode = "walk"

is_mode_available(locations, current_modes, subtour, subtour_mode)



for s in range(10):

    available_modes = ["walk", "car"]
    mode = random.choice(available_modes)
    modes = [mode]*(len(locations)-1)
    
    possible_modes.append(modes)
        
    for i in range(10):
        
        subtour_index = random.choice(list(range(len(subtours))))
        subtour = subtours[subtour_index]
        
        available_modes = []
        
        for m in ["walk", "car"]:
            if is_mode_available(locations, modes, subtour, m):
                available_modes.append(m)
        
        if len(available_modes) > 0:    
            subtour_mode = random.choice(available_modes)
            for j in subtour[:-1]:
                modes[j] = subtour_mode
        
        possible_modes.append(list(modes))
        
            
df = pd.DataFrame(possible_modes).drop_duplicates()
print(df)
