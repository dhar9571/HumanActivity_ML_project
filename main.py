from Operation.processor import processor



def main():
    try:
        processor()
        
    except Exception as e:
        print(f"Exception caught {e}")
    
    
if __name__ == "__main__":
    main()