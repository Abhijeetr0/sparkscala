object ans23 {
  def main(args: Array[String]): Unit = {
    var PurchaseAmount=180
    var hasLoyaltyCard= true
    if(PurchaseAmount>150){
      print(" customer is eligible for a discount")
    }else if( hasLoyaltyCard){
      print("qualifies for membership benefits")
    }else{
      print("customer is not eligible for a discount or membership benefits")
    }

  }

}
