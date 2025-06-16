import argparse

import lxml.etree as ElementTree


def combine_xtce_files(file1, file2, output_file):
    """
    Combine two XTCE XML files into one, merging their contents.

    Args:
        file1 (str): Path to the first XTCE XML file.
        file2 (str): Path to the second XTCE XML file.
        output_file (str): Path where the combined XTCE XML file will be saved.
    """
    # Parse the first XTCE file
    tree1 = ElementTree.parse(
        file1, parser=ElementTree.XMLParser(remove_blank_text=True)
    )
    root1 = tree1.getroot()

    # Parse the second XTCE file
    tree2 = ElementTree.parse(
        file2, parser=ElementTree.XMLParser(remove_blank_text=True)
    )
    root2 = tree2.getroot()

    # Merge the contents of the second file into the first
    for child1, child2 in zip(root1, root2):
        for grandchild1, grandchild2 in zip(child1, child2):
            for item in grandchild2:
                grandchild1.append(item)

    # Write the combined content to the output file
    tree1.write(
        output_file,
        pretty_print=True,
        xml_declaration=True,
        encoding="UTF-8",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Combine two XTCE XML files into one.")
    parser.add_argument("file1", help="Path to the first XTCE XML file.", type=str)
    parser.add_argument("file2", help="Path to the second XTCE XML file.", type=str)
    parser.add_argument(
        "output_file",
        help="Path where the combined XTCE XML file will be saved.",
        type=str,
    )
    args = parser.parse_args()

    combine_xtce_files(args.file1, args.file2, args.output_file)
    print(f"Combined XTCE files saved to {args.output_file}")
