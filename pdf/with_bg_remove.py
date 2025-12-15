"""
PDF Image Insertion Script with Background Removal
Removes background from images and adds them to PDF with transparency
"""

from reportlab.pdfgen import canvas
from PyPDF2 import PdfReader, PdfWriter
from PIL import Image
from rembg import remove
import io

def remove_image_background(image_path, output_path=None):
    """
    Remove background from an image
    
    Args:
        image_path: Path to input image
        output_path: Optional path to save the processed image
    
    Returns:
        Path to processed image (or temp path if output_path not provided)
    """
    print(f"Removing background from: {image_path}")
    
    # Open the image
    with open(image_path, 'rb') as input_file:
        input_data = input_file.read()
    
    # Remove background
    output_data = remove(input_data)
    
    # Save or return the image
    if output_path is None:
        output_path = image_path.rsplit('.', 1)[0] + '_nobg.png'
    
    with open(output_path, 'wb') as output_file:
        output_file.write(output_data)
    
    print(f"Background removed, saved to: {output_path}")
    return output_path


def add_images_to_pdf(input_pdf, output_pdf, image_configs, remove_bg=True):
    """
    Add images to a PDF at specified positions
    
    Args:
        input_pdf: Path to input PDF file
        output_pdf: Path to output PDF file
        image_configs: List of dicts with image configuration
            Each dict should have:
            - 'page': Page number (0-indexed)
            - 'image_path': Path to image file
            - 'x': X coordinate (from bottom-left)
            - 'y': Y coordinate (from bottom-left)
            - 'width': Image width
            - 'height': Image height
            - 'remove_bg': (optional) Override global remove_bg setting
        remove_bg: Whether to remove background from images (default: True)
    """
    
    # Read the original PDF
    reader = PdfReader(input_pdf)
    writer = PdfWriter()
    
    # Process images and remove backgrounds if needed
    processed_configs = []
    for config in image_configs:
        processed_config = config.copy()
        
        # Check if we should remove background for this image
        should_remove_bg = config.get('remove_bg', remove_bg)
        
        if should_remove_bg:
            # Remove background and update path
            processed_image_path = remove_image_background(config['image_path'])
            processed_config['image_path'] = processed_image_path
        
        processed_configs.append(processed_config)
    
    # Group images by page for efficiency
    images_by_page = {}
    for config in processed_configs:
        page_num = config['page']
        if page_num not in images_by_page:
            images_by_page[page_num] = []
        images_by_page[page_num].append(config)
    
    # Process each page
    for page_num in range(len(reader.pages)):
        page = reader.pages[page_num]
        
        # If this page has images to add
        if page_num in images_by_page:
            # Create a new PDF with the images
            packet = io.BytesIO()
            
            # Get page dimensions
            page_width = float(page.mediabox.width)
            page_height = float(page.mediabox.height)
            
            # Create canvas with same dimensions
            can = canvas.Canvas(packet, pagesize=(page_width, page_height))
            
            # Add each image for this page
            for img_config in images_by_page[page_num]:
                try:
                    can.drawImage(
                        img_config['image_path'],
                        img_config['x'],
                        img_config['y'],
                        width=img_config['width'],
                        height=img_config['height'],
                        preserveAspectRatio=True,
                        mask='auto'  # This ensures transparency is preserved
                    )
                except Exception as e:
                    print(f"Error adding image {img_config['image_path']}: {e}")
            
            can.save()
            
            # Move to the beginning of the BytesIO buffer
            packet.seek(0)
            
            # Read the new PDF with images
            overlay_pdf = PdfReader(packet)
            overlay_page = overlay_pdf.pages[0]
            
            # Merge the overlay with the original page
            page.merge_page(overlay_page)
        
        # Add the page to the output
        writer.add_page(page)
    
    # Write the output PDF
    with open(output_pdf, 'wb') as output_file:
        writer.write(output_file)
    
    print(f"\n✓ PDF created successfully: {output_pdf}")


def process_single_image(image_path, output_path):
    """
    Helper function to process a single image without adding to PDF
    Useful for previewing the background removal result
    """
    remove_image_background(image_path, output_path)
    print(f"Preview your image at: {output_path}")


# Example usage
if __name__ == "__main__":
    
    # Option 1: Just remove background from images (for preview)
    # Uncomment to test background removal first
    # process_single_image('image1.png', 'image1_preview.png')
    
    # Option 2: Add images to PDF with background removal
    image_configs = [
        {
            'page': 0,  # First page (0-indexed)
            'image_path': 'P.png',
            'x': 320,  # X position from left
            'y': 650,  # Y position from bottom
            'width': 100,
            'height': 50
            # 'page': 0,  # First page (0-indexed)
            # 'image_path': 'image1.png',
            # 'x': 100,  # X position from left
            # 'y': 500,  # Y position from bottom
            # 'width': 200,
            # 'height': 150
        },
        # {
        {

            'page': 0,  # First page (0-indexed)
            'image_path': 'P.png',
            'x': 100,  # X position from left
            'y': 625,  # Y position from bottom
            'width': 100,
            'height': 50
        },
        {

            'page': 0,  # First page (0-indexed)
            'image_path': 'P.png',
            'x': 210,  # X position from left
            'y': 625,  # Y position from bottom
            'width': 100,
            'height': 50
        },
        {

            'page': 0,  # First page (0-indexed)
            'image_path': 'P.png',
            'x': 350,  # X position from left
            'y': 625,  # Y position from bottom
            'width': 100,
            'height': 50
        },
        {

            'page': 0,  # First page (0-indexed)
            'image_path': 'P.png',
            'x': 60,  # X position from left
            'y': 600,  # Y position from bottom
            'width': 100,
            'height': 50
        },
        
        
        
        #     'page': 0,  # First page
        #     'image_path': 'image2.jpg',
        #     'x': 350,
        #     'y': 500,
        #     'width': 200,
        #     'height': 150,
        #     'remove_bg': True  # You can specify per-image
        # },
        # {
        #     'page': 1,  # Second page
        #     'image_path': 'logo.png',
        #     'x': 100,
        #     'y': 300,
        #     'width': 150,
        #     'height': 150,
        #     'remove_bg': False  # Keep background for this one
        # }
    ]
    
    # Process the PDF
    # Set remove_bg=False if you want to keep backgrounds by default
    add_images_to_pdf(
        input_pdf='k.pdf',
        output_pdf='output_images.pdf',
        image_configs=image_configs,
        remove_bg=True  # Global setting for background removal
    )
    
    print("\n📋 Tips:")
    print("- Images with removed backgrounds will look cleaner on PDF")
    print("- Use 'remove_bg': False for specific images to keep their background")
    print("- Preview images first using process_single_image() function")
    print("- Adjust x, y coordinates based on your PDF layout")