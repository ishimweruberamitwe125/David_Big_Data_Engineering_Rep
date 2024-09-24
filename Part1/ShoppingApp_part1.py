import sys
class ShoppingCart:
    def __init__(self):
        self.items = {
            1: {"Item": "Biscuit", "Quantity": 5,"Cost/Item": 20.5, },
            2: {"Item": "Cereals", "Quantity": 10, "Cost/Item": 90},
            3: {"Item": "Chicken", "Quantity": 20, "Cost/Item": 100}}
        self.cart = {}
    def display_menu(self):
        print("\n" + "=" * 40)
        print("Welcome to David  Shop")
        print("=" * 40)
        print("1. View Items")
        print("2. Add Item to Cart")
        print("3. View Cart")
        print("4. Remove Item from Cart")
        print("5. Checkout")
        print("6. Exit")
        print("=" * 40)
    def display_items(self):
        print("\n" + "=" * 50)
        print(f"{'Sr.no':<10}{'Item':<15}{'Quantity':<10}{'Cost/Item':<15}")
        print("=" * 50)
        for item_id, item_info in self.items.items():
            print(f"{item_id:<5}{item_info['Item']:<15}{item_info['Quantity']:<10}{item_info['Cost/Item']:<15}")
        print("=" * 50)
    def add_to_cart(self):
        while True:
            self.display_items()
            sr_no = int(input("Enter the Sr no of the item you want to add to cart: "))
            if sr_no in self.items:
                quantity = int(input(f"Enter the quantity of {self.items[sr_no]['Item']} (max {self.items[sr_no]['Quantity']}): "))
                if quantity <= self.items[sr_no]['Quantity']:
                    if sr_no in self.cart:
                        self.cart[sr_no] += quantity
                    else:
                        self.cart[sr_no] = quantity
                    self.items[sr_no]['Quantity'] -= quantity
                    print(f"{quantity} {self.items[sr_no]['Item']}(s) added to cart.")
                else:
                    print("Insufficient stock!")
            else:
                print("Invalid Sr no!")           
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
        print(f"{'Item':<20}{'Quantity':<15}{'Cost/Item':<15}{'Total (Rs)':<15}")
        print("=" * 60)
        total = 0
        for item_id, quantity in self.cart.items():
            item = self.items[item_id]
            item_total = item['Cost/Item'] * quantity
            total += item_total
            print(f"{item['Item']:<20}{quantity:<15}{item['Cost/Item']:<15}{item_total:<15}")
        print("=" * 60)
        print(f"{'Total:':<50}{total:<10}")
        print("=" * 60)
    def remove_from_cart(self):
        self.view_cart()
        if not self.cart:
            return
        item_id = int(input("Enter the Sr no of the item you want to remove from cart: "))
        if item_id in self.cart:
            quantity = int(input(f"Enter the quantity of {self.items[item_id]['Quantity']} to remove: "))
            if quantity <= self.cart[item_id]:
                self.cart[item_id] -= quantity
                self.items[item_id]['Quantity'] += quantity
                if self.cart[item_id] == 0:
                    del self.cart[item_id]
                print(f"{quantity} {self.items[item_id]['Item']}(s) removed from cart.")
            else:
                print("Invalid quantity!")
        else:
            print("Item not in cart!")
    def calculate_delivery_charge(self, distance):
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
        total = sum(self.items[item_id]['Cost/Item'] * quantity for item_id, quantity in self.cart.items())
        name=input("what is your name: ")
        address=input("what is your address: ")
        distance = float(input("Enter the delivery distance in km(5/10/15/20): "))
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
            print("Order placed successfully!")
            print("Hava a nice day")
            self.cart.clear()
            print("=" * 20 + "Remaining Quantity in Store" + "=" * 20)
            for item_id, item in self.items.items():
                print(f"{item['Item']:<20} {item['Quantity']:<15} {item['Cost/Item']:<15}")  
        else:
            print("Order cancelled.")
       
def main():
    shopping_cart = ShoppingCart()
    while True:
        shopping_cart.display_menu()
        choice = input("Enter your choice (1-6): ")
        if choice == '1':
            shopping_cart.display_items()
        elif choice == '2':
            shopping_cart.add_to_cart()
        elif choice == '3':
            shopping_cart.view_cart()
        elif choice == '4':
            shopping_cart.remove_from_cart()
        elif choice == '5':
            shopping_cart.checkout()
        elif choice == '6':
            print("Thank you for shopping with us!")
            sys.exit(0)
        else:
            print("Invalid choice. Please try again.")
if __name__ == "__main__":
    main()
