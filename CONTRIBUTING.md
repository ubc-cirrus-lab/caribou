# Contributing to Caribou

Thank you for your interest in contributing to Caribou! We’re excited to have you join our community of developers and researchers working together to improve serverless workflows on AWS. This document will guide you through the process of contributing to the project.

## Code of Conduct

We are committed to creating a welcoming and inclusive environment for everyone. Please be respectful, professional, and collaborative when engaging in discussions, reviewing code, and contributing to the project. Any form of harassment or discrimination will not be tolerated.

## How to Contribute

### 1. Reporting Issues

If you encounter a bug, have a feature request, or want to share ideas for improvement, feel free to open an issue in the repository.
Feel free to use the issue templates provided to ensure that your issue is well-structured and easy to understand.
Please ensure that your issue is clear and provides as much detail as possible.

- **Bug Reports**: Provide a detailed description of the issue, steps to reproduce it, and any relevant logs or error messages.
- **Feature Requests**: Describe the feature or enhancement you'd like to see and explain why it would be beneficial.

### 2. Working on Issues

Before you start working on an issue:

- **Assign it to yourself**: This helps avoid duplication of effort and allows others to see that the issue is being actively worked on.
If the issue is not assigned, feel free to claim it.
- **Communication**: If you have any questions or need clarification, don’t hesitate to start a discussion in the issue thread.

### 3. Submitting Pull Requests (PRs)

When you are ready to submit a Pull Request (PR):

- **Assign the PR to yourself**: This indicates that you are the primary author of the changes.
- **Request Reviews**: Ask for at least one approval from a project maintainer or a contributor.
Ensure your code follows the project's guidelines and passes all tests.
- **Do Not Close Comments**: When receiving feedback, do not close comments on the PR yourself.
Leave it to the commenter to decide whether their concerns have been addressed and resolved.
- **Add a good description**: Provide a clear and concise description of the changes made in the PR.
Mention any relevant issues or PRs that are related to your changes.
If an issue is being addressed and resolved, include `Closes #issue-number` in the PR description.
- **Squash Commits**: If your PR has multiple commits, always squash them into a single commit before merging.
GitHub provides an option to squash commits when merging a PR.
This also reduces the need for overly verbose commit messages and keeps the commit history clean.

### 4. Code Style

Please follow the existing code style used in the repository.
We have automated checks in place to ensure that the code adheres to the style guidelines.
Please always run the linting and formatting checks before submitting your PR using the `./scripts/compliance.sh` script.
Ensure that your code is:

- **Readable**: Write code that is easy to understand and maintain.
- **Documented**: Add comments where necessary to explain complex logic or workflows.
- **Tested**: Write tests for new features and bug fixes. Ensure that existing tests pass before submitting your PR.

### 5. Be Friendly and Welcoming

We value a positive and collaborative atmosphere. Always be friendly, patient, and considerate towards others.
If you are reviewing code or responding to an issue, provide constructive feedback and help contributors learn and grow.

## Getting Started

To get started with contributing to Caribou, follow these steps:

1. **Fork the repository**: Create a personal copy of the repository by forking it.
2. **Clone the repository**: Clone your fork to your local machine:

   ```bash
   git clone https://github.com/your-username/caribou.git
   cd caribou
   ```

3. **Install dependencies**: Ensure you have the necessary dependencies installed by following the instructions in the [`INSTALL.md`](./INSTALL.md) file.
4. **Create a branch**: Create a new branch for your work:

   ```bash
   git checkout -b username/type-of-work/short-description
   ```

   The type of work can be `feature`, `bugfix`, `refactor`, etc.

5. **Make your changes**: Work on your changes in your branch. Make sure to test them thoroughly.
6. **Push your changes**: Once you are happy with your changes, push them to your forked repository:

   ```bash
   git push origin username/type-of-work/short-description
   ```

7. **Open a PR**: Open a Pull Request against the main branch, following the PR submission guidelines mentioned above.

## Review Process

- PRs will be reviewed by maintainers or contributors who have expertise in the relevant area.
- Feedback will be provided, and changes may be requested before approval.
- Once approved by at least one reviewer, the PR will be merged by a maintainer.

## Questions and Discussions

If you have any questions or ideas, feel free to reach out through the issue tracker or by starting a discussion. We encourage open communication and collaboration!

Thank you for contributing to Caribou. Your contributions help make the project stronger and more impactful.

Happy coding!

---

**Caribou Maintainers**
