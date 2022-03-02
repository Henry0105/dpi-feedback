package com.mob.dpi.pojo;

import java.util.ArrayList;
import java.util.List;

/**
 * 定义一个实现Comparable接口的List存放Row的字段，按id升序排列，方便进行ORC写入
 *
 * Created by xcl on 2019/7/26 19:39.
 */
public class ComparableList<T> extends ArrayList implements Comparable {

	@Override
	public int compareTo(Object o){
		String id_0 = (String)this.get(0);
		String id = (String)((List)o).get(0);

		// 按list第一个元素（id）升序排列
		return id_0.compareTo(id);
	}

}
