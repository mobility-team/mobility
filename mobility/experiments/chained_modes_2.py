import itertools
import random

trips = ["home_start", "school", "work", "shopping", "work", "other", "leisure", "act1", "act2", "leisure", "other", "act3", "other", "home_end"]
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

# trips = [
#     "home_start", "a",
#     "b", "c", "d", "b",
#     "e", "f", "act", "f", "c",
#     "g", "home_end"
# ]

subpaths = []
cycles = []

while len(trips) > 0:
    
    print(trips)
    
    # Find the degree of each node
    nodes_degree = {}
    
    for i in range(len(trips)):
        
        if trips[i] not in nodes_degree.keys():
            nodes_degree[trips[i]] = 0
            
        nodes_degree[trips[i]] += 1
        
    # If all nodes have a degree of 1, there are no secondary cycles so we can
    # just create the primary home -> ... -> home cycle and stop there
    if not any([d > 1 for d in nodes_degree.values()]):
        subpaths.append(trips)
        trips = []
        
    else:
    
        # Else break up the trip chains on every node that has a degree > 1
        # (which means at least one secondary cycle starts here)
        path = []
        paths = []
        
        for i in range(len(trips)): 
            
            path.append(trips[i])
            
            if nodes_degree[trips[i]] > 1:
                paths.append(path)
                path = [trips[i]]
                
                
        paths.append(path)
        
        # Secondary cycles can be detected because they have the same node as
        # first and last nodes
        remaining_paths = []
        cycle_detected = False
        
        # print(x)
        
        for i in range(len(paths)):
            
            if paths[i][0] == paths[i][-1]:
                cycles.append(paths[i])
                cycle_detected = True
            else:
                remaining_paths.append(paths[i])
        
        # If no cycles were detected, no need to iterate all subpaths can be extracted as is
        if cycle_detected is False:
            subpaths.extend(remaining_paths)
            trips = []
        else:
            # All remaing trips are remerged together so we can reapply the secondary 
            # cycle detection algo above
            trips = list(itertools.chain(*[p if "home_end" in p else p[0:(len(p)-1)] for p in remaining_paths])) 

# stop        

visited_nodes = []
is_on_main_path = []  

for sp in subpaths:
    
    if any([n in visited_nodes for n in sp[1:len(sp)]]) is True:
        is_on_main_path.append(False)
    else:
        is_on_main_path.append(True)
        
    visited_nodes.extend(sp)
    
main_path = list(itertools.chain(*[subpaths[i][0:(len(subpaths[i])-1)] for i in range(len(subpaths)) if is_on_main_path[i] is True] ))
main_path.append("home_end")    
    
subpaths = [subpaths[i] for i in range(len(subpaths)) if is_on_main_path[i] is False]


main_path_modes = [random.choices(["car", "walk"], k=1)[0]]*len(main_path)





