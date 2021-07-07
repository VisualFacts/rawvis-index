package gr.athenarc.imsi.visualfacts.queryER.Utilities;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class Converter {


	public static long[] convertCollectionToArray(Collection<Long> ids) {
		int index = 0;
		long[] array = new long[ids.size()];
		for (Long id : ids) {
			array[index++] = id;
		}

		return array;
	}

	public static long[] convertListToArray(List<Long> set) {
		int index = 0;
		long[] array = new long[set.size()];
		for (Long id : set) {
			array[index++] = id;
		}

		return array;
	}
	
	public static long[] convertSetToArray(Set<Long> set) {
		int index = 0;
		long[] array = new long[set.size()];
		for (Long id : set) {
			array[index++] = id;
		}

		return array;
	}

	public static int getSortedListsOverlap(int[] list1, int[] list2) {
		if (list1 == null || list2 == null) {
			return 0;
		}
		int commonEntities = 0;
		for (int i = 0; i < list1.length; i++) {
			for (int j = 0; j < list2.length; j++) {
				if (list2[j] < list1[i]) {
					continue;
				}

				if (list1[i] < list2[j]) {
					break;
				}

				if (list1[i] == list2[j]) {
					commonEntities++;
				}
			}
		}
		return commonEntities;
	}

	public static List<Integer> getSortedListsOverlapingIds(int[] list1, int[] list2) {
		final List<Integer> commonIds = new ArrayList<Integer>();
		if (list1 != null && list2 != null) {
			for (int i = 0; i < list1.length; i++) {
				for (int j = 0; j < list2.length; j++) {
					if (list2[j] < list1[i]) {
						continue;
					}

					if (list1[i] < list2[j]) {
						break;
					}

					if (list1[i] == list2[j]) {
						commonIds.add(list1[i]);
					}
				}
			}
		}
		return commonIds;
	}

	public static long[] mergeAndSortArrays(long[] ids1, long[] ids2) {
		//get distinct entity ids
		final Set<Long> allIds = new HashSet<Long>();
		for (long entityId : ids1) {
			allIds.add(entityId);
		}
		for (long entityId : ids2) {
			allIds.add(entityId);
		}

		//sort distinct entity ids
		List<Long> orderedEntities = new ArrayList<Long>(allIds);
		Collections.sort(orderedEntities);

		//convert them to array
		return convertListToArray(orderedEntities);
	}

	
}