package gr.athenarc.imsi.visualfacts.queryER.Utilities;

//simple union-find based on int[] arrays
//for  "parent" and "rank"
//implements the "disjoint-set forests" described at
//http://en.wikipedia.org/wiki/Disjoint-set_data_structure
//which have almost constant "amortized" cost per operation
//(actually O(inverse Ackermann))
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class UnionFind {

	//private int[] _parent;
	//private int[] _rank;
	public HashMap<Long,Long> _parent = new HashMap<>();
	private HashMap<Long,Long> _rank = new HashMap<>();

	public Map<Long,Long> getParent(){
		return _parent;
	}


	public long find(long i) {

		if(!_parent.containsKey(i)) {
			_parent.put(i, i);
		}
		long p = _parent.get(i);
		if (i == p) {
			return i;
		}

		long findP = find(p);
		_parent.put(i, findP);

		return _parent.get(i);

	}


	public void union(long i, long j) {

		long root1 = find(i);
		long root2 = find(j);

		if (root2 == root1) return;

		if(!get_rank().containsKey(root1))
			get_rank().put(root1, new Long(0));
		if(!get_rank().containsKey(root2))
			get_rank().put(root2, new Long(0));

		if (get_rank().get(root1) > get_rank().get(root2)) {
			_parent.put(root2, root1);
		} else if ( get_rank().get(root2) >  get_rank().get(root1)) {
			_parent.put(root1, root2);
		} else {
			_parent.put(root2, root1);
			get_rank().put(root1, get_rank().get(root1) + 1);
		}
	}


	public UnionFind(Set<Long> qIds) {

		for (long i : qIds) {
			_parent.put(i, i);
			get_rank().put(i, new Long(0));
		}
	}


	public HashMap<Long,Long> get_rank() {
		return _rank;
	}


	public void set_rank(HashMap<Long,Long> _rank) {
		this._rank = _rank;
	}

}