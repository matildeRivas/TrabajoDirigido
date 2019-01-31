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
	g = Graph(directed=False)
	g.add_vertices(9)
	g.add_edges(((0, 1), (1, 2), (2, 3), (0, 4), (4, 3), (4, 5), (3, 5), (3, 7), (5, 6), (6, 7), (7, 8)))
	g.es["weight"] = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]

	#suurballe(g, 0, False)
	main_suurballe("mapa.ncol")


def main_suurballe(graph_file):
	g = read(graph_file, format='ncol', directed=False)
	layout = g.layout("kk")
	g.vs["label"] = g.vs["name"]
	g.es["label"] = g.es["weight"]
	plot(g, layout=layout, bbox=(1000, 1000), margin=200)
	# todo: por cada punto en puntos por conectar, hacer suurballe. Guardar info
	print(suurballe(g, 0, False))


def map_to_graph(graph_file):
	g = read(graph_file, format='ncol', directed=False)


def suurballe(graph, vertex, directed=False):
	graph.es["label"] = graph.es["weight"]
	graph.to_directed()
	edge_path = graph.get_shortest_paths(vertex, weights="weight", output="epath")
	tree_edges = []
	for e in edge_path:
		tree_edges = list(set(tree_edges) | set(e))
	distance = graph.shortest_paths(vertex, weights="weight")[0]
	shortest_path = get_edges(graph, edge_path)
	problem_vertex = []
	# create transformed graph Gv for each V. Future: one unified graph
	result = []

	for v in range(len(graph.vs)):
		if (distance[v] != float("inf") and v != vertex):
			gv = Graph(directed=True)
			gv.add_vertices(len(graph.vs))
			reverse_edges(gv, graph, edge_path, v)
			gv.es["weight"] = transform_costs(graph, distance)
			transformed_path = gv.get_shortest_paths(0, gv.vs[v], weights="weight", output="epath")
			shortest_transformed = get_edges(gv, transformed_path)[0]
			# unify paths discarding edges that are in both paths
			for (x, y) in shortest_transformed:
				if (y, x) in shortest_path[v]:
					shortest_path[v].remove((y, x))
					shortest_transformed.remove((x, y))
			union = shortest_path[v] + shortest_transformed
			path_1 = get_path(union, vertex, v)
			try:
				path_2 = get_path(union, vertex, v)
				result.append((path_1, path_2))
			except Exception:
				result.append((path_1))
				problem_vertex.append(v)

		else:
			result.append(())

	return result


def reverse_edges(gv, graph, edge_path, v):
	for e in range(len(graph.get_edgelist())):
		edge = graph.es[e]
		source = edge.source
		target = edge.target
		if e in edge_path[v]:
			gv.add_edge(target, source)
		else:
			gv.add_edge(source, target)


# plot(gv, layout=layout)

def get_edges(graph, edge_path):
	edges = graph.get_edgelist()
	edges_as_pairs = []
	for path in edge_path:
		path_pairs = []
		for e in path:
			path_pairs.append(edges[e])
		edges_as_pairs.append(path_pairs)
	return edges_as_pairs


def get_path(edges, source, target):
	path = []
	edges.sort(key=lambda x: (x[1], x[0]))
	try:
		current = search_tuple(edges, target)
		path.append(current)
		while (current[0] != source):
			current = search_tuple(edges, current[0])
			path.append(current)
	except Exception:
		raise Exception
	return path


def search_tuple(tups, elem):
	result = list(filter(lambda tup: tup[1] == elem, tups))
	try:
		tups.remove(result[0])
		return result[0]
	except IndexError:
		raise Exception


# todo: raise exception


def transform_costs(graph, distance):
	transformed_costs = []
	for i in range(len(graph.es)):
		d_target = distance[graph.es[i].target]
		d_source = distance[graph.es[i].source]
		if d_target == float("inf") or d_source == float("inf"):
			transformed_costs.append(float("inf"))
		else:
			transformed_costs.append(graph.es[i]["weight"] - d_target + d_source)
	return transformed_costs


if __name__ == "__main__":
	main(sys.argv)
