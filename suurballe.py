from igraph import *
import psycopg2
from psycopg2.extensions import AsIs
import sys
import pprint
from config import config


def main(argv):
	'''if (len(sys.argv) < 2 or len(sys.argv) > 2):
		print("Debe ingresar nombre de archivo con lista de aristas y costos")
	else:
		suurballe(sys.argv[1], 1)
	'''
	g = Graph(directed=True)
	g.add_vertices(8)
	g.add_edges(((0, 1), (0, 2), (0,4),(1,4), (1,5), (1,3), (2,5), (3,6), (4,6), (5,7), (7,2), (7, 6)))
	g.es["weight"] = [3, 2, 8, 4, 6, 1, 5, 5, 1, 2, 3, 7 ]
	suurballe(g, 0)


'''def main_suurballe(graph_file):
	g = read(graph_file, format='ncol', directed=False)
	layout = g.layout("kk")
	# g.vs["label"] = g.vs["name"]
	#plot(g, layout=layout)
	#todo por cada punto en puntos por conectar, hacer suurballe. Guardar info
'''

def suurballe(graph, vertex):

	graph.es["label"] = graph.es["weight"]
	#graph.to_directed()
	edge_path = graph.get_shortest_paths(vertex, weights="weight", output="epath")
	tree_edges = []
	for e in edge_path:
		tree_edges = list(set(tree_edges) | set(e))
	#shortest_path = graph.get_shortest_paths(vertex, weights="weight")
	distance = graph.shortest_paths(vertex, weights="weight")[0]
	shortest_path = get_edges(graph, edge_path)
	# do cost transformation
	transformed_costs = []
	for i in range(len(graph.es)):
		transformed_costs.append(graph.es[i]["weight"] - distance[graph.es[i].target] + distance[graph.es[i].source])
	# create transformed graph Gv for each V. Future: one unified graph
	for v in range(1, len(graph.vs)) :
		if (distance[v] != float("inf") and v == 6):
			gv = Graph(directed=True)
			gv.add_vertices(len(graph.vs))
			#gv.add_edges(graph.get_edgelist())
			#gv.delete_edges(edge_path[v])
			for e in range(len(graph.get_edgelist())):
				edge = graph.es[e]
				source = edge.source
				target = edge.target
				if e in edge_path[v]:
					gv.add_edge(target, source)
				else :
					gv.add_edge(source, target)
			gv.es["weight"] = transformed_costs
			transformed_path = gv.get_shortest_paths(0, gv.vs[v], weights="weight", output="epath")
			shortest_transformed = get_edges(gv, transformed_path)[0]
			# unify paths discarding edges that are in both paths
			for (x, y) in shortest_transformed:
				if (y, x) in shortest_path[v]:
					shortest_path[v].remove((y,x))
					shortest_transformed.remove((x,y))
			union = shortest_path[v] + shortest_transformed
			path_1 = get_path(union, vertex, v)
			path_2 = get_path(union, vertex, v)
			print(path_1, path_2)

def get_edges(graph, edge_path):
	edges = graph.get_edgelist()
	shortest_path = []
	for path in edge_path:
		path_pairs = []
		for e in path:
			path_pairs.append(edges[e])
		shortest_path.append(path_pairs)
	return shortest_path

def get_path(edges, source, target):
	path = []
	edges.sort(key=lambda x: (x[1], x[0]))
	current = search_tuple(edges, target)
	path.append(current)
	while(current[0] != source):
		current = search_tuple(edges, current[0])
		path.append(current)
	return path


def search_tuple(tups, elem):
	result = list(filter(lambda tup: tup[1]==elem, tups))
	tups.remove(result[0])
	return result[0]


if __name__ == "__main__":
	main(sys.argv)