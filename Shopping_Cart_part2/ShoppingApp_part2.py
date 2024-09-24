import sys
import mysql.connector
class ShoppingCart:
    def __init__(self):
        # MySQL connection configuration
        self.db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': 'IshimweRuberamitwe@125',
            'database': 'shopping_cart'}
        # Initialize MySQL connection
        self.conn = mysql.connector.connect(**self.db_config)
        self.cursor = self.conn.cursor(dictionary=True)
        # Shopping cart to store selected items
        self.cart = {}
    def display_menu(self):
        print("\n" + "=" * 40)
        print("Welcome to David Shop part 2 using mysql database")
        print("=" * 40)
        print("1. View Items in the database")
        print("2. Add Item to Cart")
        print("3. View Cart")
        print("4. Remove Item from Cart")
        print("5. Checkout")
        print("6. Exit")
        print("=" * 40)
    def display_items(self):
        self.cursor.execute("SELECT * FROM items")
        items = self.cursor.fetchall()
        print("\n" + "=" * 50)
        print(f"{'Sr.no':<10}{'Item':<15}{'Cost':<15}{'Quantity':<10}")
        print("=" * 50)
        for item in items:
            print(f"{item['id']:<10}{item['Item']:<15}{item['Cost']:<15}{item['Quantity']:<10}")
        print("=" * 50)
    def add_to_cart(self):
        while True:
            self.display_items()
            item_id = int(input("Enter the Sr no of the item you want to add to cart: "))
            self.cursor.execute("SELECT * FROM items WHERE id = %s", (item_id,))
            item = self.cursor.fetchone()
            if item:
                quantity = int(input(f"Enter the quantity of {item['Item']} (max {item['Quantity']}): "))
                if quantity <= item['Quantity']:
                    if item_id in self.cart:
                        self.cart[item_id] += quantity
                    else:
                        self.cart[item_id] = quantity
                    # Update stock in database
                    self.cursor.execute("UPDATE items SET Quantity = Quantity - %s WHERE id = %s", (quantity, item_id))
                    self.conn.commit()
                    print(f"{quantity} {item['Item']}(s) added to cart.")
                else:
                    print("Insufficient stock!")
            else:
                print("Invalid item Sr no!")

            continue_shopping = input("Do you want to continue shopping? (yes/no): ").lower()
            if continue_shopping != 'yes':
                break
    def view_cart(self):
        if not self.cart:
            print("Your cart is empty.")
            return
        print("\n" + "=" * 60)
        print("Your Shopping Cart")
        print("=" * 60)
        print(f"{'Item':<20}{'Quantity':<15}{'Cost':<15}{'Total':<15}")
        print("=" * 60)
        total = 0
        for item_id, quantity in self.cart.items():
            self.cursor.execute("SELECT * FROM items WHERE id = %s", (item_id,))
            item = self.cursor.fetchone()
            item_total = item['Cost'] * quantity
            total += item_total
        print(f"{item['Item']:<20}{quantity:<15}{item['Cost']:<15}{item_total:<15}")
        print("=" * 60)
        print(f"{'Total:':<50}{total:<10}")
        print("=" * 60)

    def remove_from_cart(self):
        self.view_cart()
        if not self.cart:
            return
        item_id = int(input("Enter the Sr no of the item you want to remove from cart: "))
        if item_id in self.cart:
            self.cursor.execute("SELECT * FROM items WHERE id = %s", (item_id,))
            item = self.cursor.fetchone()
            quantity = int(input(f"Enter the quantity of {item['Item']} to remove: "))
            if quantity <= self.cart[item_id]:
                self.cart[item_id] -= quantity
                
                # Update stock in database
                self.cursor.execute("UPDATE items SET Quantity = Quantity2 + %s WHERE id = %s", (quantity, item_id))
                self.conn.commit()
                
                if self.cart[item_id] == 0:
                    del self.cart[item_id]
                print(f"{quantity} {item['Item']}(s) removed from cart.")
            else:
                print("Invalid quantity!")
        else:
            print("Item not in cart!")

    def calculate_delivery_charge(self,distance):
        if distance <= 15:
            return 50
        elif 15 < distance <= 30:
            return 100
        else:
            return None

    def checkout(self):
        print("=" * 20+"Bill"+"="*20)
        self.view_cart()
        if not self.cart:
            return
        
        total = 0
        for item_id, quantity in self.cart.items():
            self.cursor.execute("SELECT Cost FROM items WHERE id = %s", (item_id,))
            item = self.cursor.fetchone()
            total += item['Cost'] * quantity
        name=input("what is your name: ")
        address=input("what is your address: ")
        distance = float(input("Enter the delivery distance in km: "))
        delivery_charge = self.calculate_delivery_charge(distance)
        
        if delivery_charge is None:
            print("Sorry, delivery is not available for this distance.")
            return
        grand_total = total + delivery_charge
        print("\n" + "=" * 40)
        print("Checkout Summary")
        print(f"Total item cost {total}")
        print(f"Total Bill amount:Total items cost + Delivery Charge is  {grand_total}")
        print(f"Name :     {name}")
        print(f"Address:   {address}")
        print("=" * 40)
        confirm = input("Confirm order? (yes/no): ").lower()
        if confirm == 'yes':
            # Create order in database
            self.cursor.execute("INSERT INTO orders (total_amount, delivery_charge) VALUES (%s, %s)", (grand_total, delivery_charge))
            order_id = self.cursor.lastrowid
            
            # Create order items in database
            for item_id, quantity in self.cart.items():
                self.cursor.execute("INSERT INTO order_items (order_id, item_id, quantity) VALUES (%s, %s, %s)", (order_id, item_id, quantity))
            
            self.conn.commit()
            print("Order placed successfully!")
            self.cart.clear()
            print("=" * 20 + "Remaining Quantity in Store" + "=" * 20)
            self.cursor.execute("SELECT * FROM items")
            items = self.cursor.fetchall()
            for item in items:
                print(f"{item['id']:<10}{item['Item']:<15}{item['Cost']:<15}{item['Quantity']:<10}")
        else:
            print("Order cancelled.")

def main():
    cart=ShoppingCart()
    while True:
        cart.display_menu()
        choice = input("Enter your choice (1-6): ")
        if choice == '1':
            cart.display_items()
        elif choice == '2':
            cart.add_to_cart()
        elif choice == '3':
            cart.view_cart()
        elif choice == '4':
            cart.remove_from_cart()
        elif choice == '5':
            cart.checkout()
        elif choice == '6':
            print("Thank you for shopping with us!")
            cart.cursor.close()
            cart.conn.close()
            sys.exit(0)
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()