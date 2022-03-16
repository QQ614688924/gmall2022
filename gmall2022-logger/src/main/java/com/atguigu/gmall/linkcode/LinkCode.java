package com.atguigu.gmall.linkcode;

public class LinkCode {

    public static void main(String[] args) {

        ListNode head = new ListNode(1);
        ListNode head2 = new ListNode(2);
        ListNode head3 = new ListNode(3);
        ListNode head4 = new ListNode(4);
        ListNode head5 = new ListNode(5);

        head.next = head2;
        head2.next = head3;
        head3.next = head4;
        head4.next = head5;

//        reverseBetween(head, 2, 4);
        reverseListNode(head);
    }

    public static ListNode reverseListNode(ListNode head) {

        ListNode newHead = null;

        while (head != null) {
            ListNode temp = head.next;
            head.next = newHead;
            newHead = head;
            head = temp;
        }

        return newHead;
    }


    public static ListNode reverseBetween(ListNode head, int m, int n) {
        // 定义一个dummyHead, 方便处理
        ListNode dummyHead = new ListNode(0);
        dummyHead.next = head;

        // 初始化指针
        ListNode g = dummyHead;
        ListNode p = dummyHead.next;

        // 将指针移到相应的位置
        for (int step = 0; step < m - 1; step++) {
            g = g.next;
            p = p.next;
        }

        // 头插法插入节点
        for (int i = 0; i < n - m; i++) {
            ListNode removed = p.next;
            p.next = p.next.next;

            removed.next = g.next;
            g.next = removed;
        }

        return dummyHead.next;
    }

    public static class ListNode {
        int val;
        ListNode next;   // 下一个链表对象

        ListNode(int x) {
            val = x;
        }  //赋值链表的值

        @Override
        public String toString() {
            return "ListNode{" +
                    "val=" + val +
                    ", next=" + next +
                    '}';
        }
    }
}
