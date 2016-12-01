题目1： 根据链表的定义，编写函数printLinkList(LinkList head)，实现打印一个给定链表的功能。（25分）
```
	public static class LinkList{
		public int value; // 存放节点的值
		public LinkList next = null; // 存放下一个节点
		LinkList(int value){
			this.value = value;
		}
		LinkList(int value, LinkList next){
			this.value = value;
			this.next = next;
		}
		public void setValue(int value){
			this.value = value;
		}
	}
	// 实现以下函数，打印链表head
	public void printLinkList(LinkList head){
	 String s="";
  	s+=head.value;
  	LinkList temp=head.next;
  	while(temp!=null){
  		s+="->"+temp.value;
  		temp=temp.next;
  	}
  	System.out.println(s);
	}
```

 public static Node reverseLinkList(Node head)  
    {  
    	Node p1= null;
	Node p2;
		
	while(head!= null){
		p2 = head.next;
		head.next = p1;
		p1 = head;
		head = p2;
			
	}
		
      return p1;
          
    }  

打印格式如下`1->2->5->3`，具体代码规定，请参考ex1.java文件。

加分项：
编写public Node reverseLinkList(head){}函数，反转链表。（+5分）
