package org.shirdrn.streaming.utils;

public class Pair<L, R> {

	private final L left;
	private final R right;

	public Pair(L l, R r) {
		left = l;
		right = r;
	}

	public L getLeft() {
		return left;
	}

	public R getRight() {
		return right;
	}

	public static <L, R> Pair<L, R> of(L left, R right) {
		return new Pair<L, R>(left, right);
	}
	
	@Override
	public String toString() {
		return "left=" + left + ", right=" + right;
	}
}
