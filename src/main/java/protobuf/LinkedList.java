package protobuf;

public class LinkedList<T> {

    private Node<T> head; // Reference to the first node (head)
  
    // Node class to represent a single element in the list
    private static class Node<T> {
        T data;
        Node<T> next;
  
        public Node(T data) {
            this.data = data;
        }
    }
  
    // Method to append an element to the head of the list
    public void appendToHead(T data) {
        Node<T> newNode = new Node<>(data);
        newNode.next = head;
        head = newNode;
    }
  
    // Method to get the length of the linked list (number of elements)
    public int getLength() {
        int count = 0;
        Node<T> current = head;
        while (current != null) {
            count++;
            current = current.next;
        }
        return count;
    }
  
    // Method to append an element to a specific position (n) in the list
    public void appendAtPosition(T data, int position) {
        if (position < 0) {
            throw new IllegalArgumentException("Invalid position: cannot be negative");
        }
    
        if (position == 0) {
            appendToHead(data);
            return;
        }
    
        // Traverse the list to find the node before the target position
        Node<T> current = head;
        int counter = 1;
        while (current != null && counter < position) {
            current = current.next;
            counter++;
        }
    
        // Check if position is beyond the end of the list
        if (current == null) {
            throw new IndexOutOfBoundsException("Invalid position: exceeds list size");
        }
    
        // Create a new node and insert it after the node at (position-1)
        Node<T> newNode = new Node<>(data);
        newNode.next = current.next;
        current.next = newNode;
    }
}